import { Plugin, PluginEvent, PluginMeta } from '@posthog/plugin-scaffold';
import * as snowflake from 'snowflake-sdk';

import { createPool, Pool } from 'generic-pool';

declare namespace posthog {
    function capture(event: string, properties?: Record<string, any>): void
}

enum IMPORT_MECHANISMS {
    CONTINUOUS = 'Import continuously',
    HISTORICAL = 'Only import historical data'
};

type SnowflakeImportPlugin = Plugin<{
    global: {
        snowflakeClient: SnowflakeConnectionPool
        eventsToIgnore: Set<string>
        sanitizedTableName: string
        initialOffset: number
        totalRows: number
    }
    config: {
        account: string
        username: string
        password: string
        database: string
        schema: string
        table: string
        orderBy: string
        warehouse: string
        batchSize: string
        frequency: string
        eventsToIgnore: string
        role: string
        transformationName: string
        importMechanism: IMPORT_MECHANISMS
    }
}>

interface ImportEventsJobPayload extends Record<string, any> {
    offset?: number
    retriesPerformedSoFar: number
}

interface ExecuteQueryResponse {
    error: Error | null
    queryResult: Record<string, any>[] | null
}

interface TransformedPluginEvent {
    event: string,
    distinct_id?: any,
    timestamp?: Date,
    properties?: PluginEvent['properties']
}

interface TransformationsMap {
    [key: string]: {
        author: string
        transform: (row: Record<string, any>, meta: PluginMeta<SnowflakeImportPlugin>) => Promise<TransformedPluginEvent>
    }
}

/* START: SnowflakeConnectorPool Class */
interface SnowflakeOptions {
    account: string
    username: string
    password: string
    role: string
    warehouse: string
    database?: string
    schema?: string
    table?: string
}
class SnowflakeConnectionPool {
    private pool: Pool<snowflake.Connection>
    private database?: string
    private schema?: string

    constructor({
        account,
        username,
        password,
        database,
        schema,
        role,
        warehouse
    }: SnowflakeOptions) {
        this.database = database?.toUpperCase();
        this.schema = schema?.toUpperCase();
        this.pool = this.createConnectionPool(account, username, password, role, warehouse);
    }

    private createConnectionPool(
        account: string,
        username: string,
        password: string,
        role: string,
        warehouse: string
    ): SnowflakeConnectionPool['pool'] {
        return createPool(
            {
                create: async () => {
                    const connection: snowflake.Connection = snowflake.createConnection({
                        account,
                        username,
                        password,
                        database: this.database,
                        schema: this.schema,
                        role,
                        warehouse
                    });

                    console.log('Trying to connect to Snowflake');

                    await new Promise<string>((resolve, reject) =>
                        connection.connect((err, conn) => {
                            if (err) {
                                console.error('Error connecting to Snowflake: ' + err.message)
                                reject(err)
                            } else {
                                console.log('Connected to Snowflake');
                                resolve(conn.getId())
                            }
                        })
                    );

                    return connection;
                },
                destroy: async (connection) => {
                    await new Promise<void>((resolve, reject) =>
                        connection.destroy(function (err) {
                            if (err) {
                                console.error('Error disconnecting from Snowflake:' + err.message)
                                reject(err)
                            } else {
                                resolve()
                            }
                        })
                    )
                },
            },
            {
                min: 1,
                max: 1,
                autostart: true,
                fifo: true,
            }
        )
    }

    public async execute({ sqlText, binds }: { sqlText: string; binds?: snowflake.Binds }): Promise<Record<string, any>[]> {
        const snowflakeConnection = await this.pool.acquire();
        try {
            return await new Promise<Record<string, any>[]>((resolve, reject) =>
                snowflakeConnection.execute({
                    sqlText,
                    binds,
                    complete: (err, _stmt, rows) => {
                        if (err) {
                            console.error('Error executing Snowflake query: ', { sqlText, error: err.message })
                            return reject(err)
                        }

                        return resolve(rows as Record<string, any>[]);
                    },
                })
            )
        } finally {
            await this.pool.release(snowflakeConnection)
        }
    }

    public async clear(): Promise<void> {
        await this.pool.drain()
        await this.pool.clear()
    }
}
/* END: SnowflakeConnectorPool Class */

const REDIS_OFFSET_KEY = 'import_offset';
const REDIS_TOTAL_ROWS_KEY = 'total_rows_snapshot';

const sanitizeSql = (text: string): string => {
    return text.replace(/[^\w\d_]+/g, '');
}

const sanitizeSqlIdentifier = (unquotedIdentifier: string): string => {
    return `identifier('${sanitizeSql(unquotedIdentifier)}')`;
}

export const jobs: SnowflakeImportPlugin['jobs'] = {
    importAndIngestEvents: async (payload, meta) => await importAndIngestEvents(payload as ImportEventsJobPayload, meta)
}

export const setupPlugin: SnowflakeImportPlugin['setupPlugin'] = async ({ config, cache, jobs, global, storage }) => {
    const requiredConfigOptions = ['account', 'username', 'password', 'role', 'database', 'schema', 'table', 'orderBy', 'warehouse', 'batchSize', 'frequency'];
    for (const option of requiredConfigOptions) {
        if (!(option in config)) {
            throw new Error(`Required config option ${option} is missing!`)
        }
    }

    console.log('Spinning up a new instance of the plugin');

    global.snowflakeClient = new SnowflakeConnectionPool(config)

    // the way this is done means we'll continuously import as the table grows
    // to only import historical data, we should set a totalRows value in storage once
    const countColumnName = 'ROW_COUNT'.toUpperCase();
    const totalRowsResult = await executeQuery(
        `SELECT COUNT(1) AS ${countColumnName} FROM ${sanitizeSqlIdentifier(config.table)}`,
        [],
        global.snowflakeClient
    );

    if (!totalRowsResult || totalRowsResult.error || !totalRowsResult.queryResult) {
        throw new Error(`Query failed because: ${totalRowsResult.error}`);
    }

    console.log(`${totalRowsResult} total rows found`);

    global.totalRows = Number(totalRowsResult.queryResult[0][countColumnName])

    // if set to only import historical data, take a "snapshot" of the count
    // on the first run and only import up to that point
    if (config.importMechanism === IMPORT_MECHANISMS.HISTORICAL) {
        const rowCount = Number(totalRowsResult.queryResult[0][countColumnName]);
        const totalRowsSnapshot = await storage.get(REDIS_TOTAL_ROWS_KEY, null);
        if (!totalRowsSnapshot) {
            await storage.set(REDIS_TOTAL_ROWS_KEY, rowCount);
        }
        global.totalRows = Number(totalRowsSnapshot || rowCount);
    } 
    
    // used for picking up where we left off after a restart
    const offset = await storage.get(REDIS_OFFSET_KEY, 0);

    // needed to prevent race conditions around offsets leading to events ingested twice
    global.initialOffset = Number(offset);
    await cache.set(REDIS_OFFSET_KEY, Number(offset) / Number(config.batchSize));

    const frequency = Number(config.frequency);

    if (!frequency) {
        throw new Error('Invalid frequency input! Please insert a number in seconds.');
    }

    await jobs.importAndIngestEvents({ retriesPerformedSoFar: 0 }).runIn(frequency, 'seconds');
}

export const teardownPlugin: SnowflakeImportPlugin['teardownPlugin'] = async ({ config, global, cache, storage }) => {
    const redisOffset = await cache.get(REDIS_OFFSET_KEY, 0);
    const workerOffset = Number(redisOffset) * Number(config.batchSize);
    const offsetToStore = workerOffset > global.totalRows ? global.totalRows : workerOffset;
    await storage.set(REDIS_OFFSET_KEY, offsetToStore);
    await global.snowflakeClient.clear();
}

const executeQuery = async (
    query: string,
    values: any[],
    client: SnowflakeConnectionPool
): Promise<ExecuteQueryResponse> => {

    let queryResult: Record<string, any>[] | null = null;
    let error: Error | null = null;

    try {
        queryResult = await client.execute({
            sqlText: query,
            binds: values
        });
    } catch(err) {
        error = err as Error;
    }

    return { error, queryResult }
}

const importAndIngestEvents = async (
    payload: ImportEventsJobPayload,
    meta: PluginMeta<SnowflakeImportPlugin>
) => {
    const { global, cache, config, jobs } = meta
    const batchSize = Number(config.batchSize);

    if (payload.offset && payload.retriesPerformedSoFar >= 15) {
        console.error(`Import error: Unable to process rows ${payload.offset}-${
            payload.offset + batchSize
        }. Skipped them.`)
        return
    }

    let offset: number
    if (payload.offset) {
        offset = payload.offset
    } else {
        const redisIncrementedOffset = Number(await cache.get(REDIS_OFFSET_KEY, 0));
        offset = global.initialOffset + redisIncrementedOffset * batchSize
    }

    if (config.importMechanism === IMPORT_MECHANISMS.HISTORICAL && offset > global.totalRows) {
        console.log(`Done importing historical rows. Please disable or reactivate with continuous import.`);
        return
    }
    
    const query = `
        SELECT *
         FROM ${sanitizeSqlIdentifier(meta.config.table)}
         ORDER BY ${sanitizeSql(meta.config.orderBy)} ASC
         LIMIT ${batchSize}
         OFFSET :1;`;

    const values = [offset];

    const queryResponse = await executeQuery(query, values, global.snowflakeClient)

    if (!queryResponse || queryResponse.error || !queryResponse.queryResult) {
        const nextRetrySeconds = 2 ** payload.retriesPerformedSoFar * 3
        console.log(
            `Unable to process rows ${offset}-${
                offset + batchSize
            }. Retrying in ${nextRetrySeconds}. Error: ${queryResponse.error}`
        )
        await jobs
            .importAndIngestEvents({
                ...payload,
                retriesPerformedSoFar: payload.retriesPerformedSoFar + 1
            })
            .runIn(nextRetrySeconds, 'seconds')
    }

    const eventsToIngest: TransformedPluginEvent[] = []

    if (queryResponse.queryResult) {
        for (const row of queryResponse.queryResult) {
            const event = await transformations[config.transformationName].transform(row, meta)
            eventsToIngest.push(event)
        }
    }

    for (const event of eventsToIngest) {
        posthog.capture(event.event, event.properties)
    }

    console.log(
        `Processed rows ${offset}-${offset + batchSize} and ingested ${eventsToIngest.length} event${
            eventsToIngest.length != 1 ? 's' : ''
        } from them.`
    )

    await cache.incr(REDIS_OFFSET_KEY);

    const frequency = Number(config.frequency);

    await jobs.importAndIngestEvents({
        retriesPerformedSoFar: 0
    }).runIn(frequency, 'seconds');
}

const formatDateToIsoString = (date: Date): string => {
    const tzo = -date.getTimezoneOffset();
    const dif = tzo >= 0 ? '+' : '-';
    const pad = (num: number) => {
        var norm = Math.floor(Math.abs(num));
        return (norm < 10 ? '0' : '') + norm;
    };
  
    return date.getFullYear() +
        '-' + pad(date.getMonth() + 1) +
        '-' + pad(date.getDate()) +
        'T' + pad(date.getHours()) +
        ':' + pad(date.getMinutes()) +
        ':' + pad(date.getSeconds()) +
        dif + pad(tzo / 60) +
        ':' + pad(tzo % 60);
}

const toTitleCase = (str: string): string =>{
    return str.replace(
      /\w\S*/g,
      (txt) => txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase()
    );
}


// Transformations can be added by any contributor
// 'author' should be the contributor's GH username
const transformations: TransformationsMap = {
    'default': {
        author: 'yakkomajuri',
        transform: async (row, _) => {
            const { timestamp, distinct_id, event, properties } = row
            const eventToIngest = { 
                event, 
                properties: {
                    timestamp, 
                    distinct_id, 
                    ...JSON.parse(properties), 
                    source: 'snowflake_import',
                }
            }
            return eventToIngest
        }
    },
    'JSON Map': {
        author: 'yakkomajuri',
        transform: async (row, { attachments }) => {
            if (!attachments.rowToEventMap) {
                throw new Error('Row to event mapping JSON file not provided!')
            }
            
            let rowToEventMap: Record<string, string> = {}
            try {
                rowToEventMap = JSON.parse(attachments.rowToEventMap.contents.toString())
            } catch {
                throw new Error('Row to event mapping JSON file contains invalid JSON!')
            }

            const eventToIngest = {
                event: '' as any,
                properties: {} as Record<string, any> 
            }

            for (const [colName, colValue] of Object.entries(row)) {
                if (!rowToEventMap[colName]) {
                    continue
                }
                if (rowToEventMap[colName] === 'event') {
                    eventToIngest.event = colValue
                } else {
                    eventToIngest.properties[rowToEventMap[colName]] = colValue
                }
            }

            return eventToIngest
        }
    },
    'passthrough': {
        author: 'tacastillo',
        transform: async (row, _): Promise<TransformedPluginEvent> => {

            const types: Record<string, any> = {};

            for (const key of Object.keys(row)) {
                types[key] = typeof row[key];
            }

            const unformattedTimestamp = row['COMPLETED_AT_ET'];
            const timestamp = formatDateToIsoString(unformattedTimestamp);

            return {
                event: 'test_event',
                properties: row
            };
        }
    },
    'Cleaned-Up Properties': {
        author: 'tacastillo',
        transform: async (row, { attachments }) => {

            let transformationConfig: Record<string, any> = {};

            if (!attachments.propertyConfigJson) {
                throw new Error('Configuration JSON file not provided!')
            }
        
            try {
                transformationConfig = JSON.parse(attachments.propertyConfigJson.contents.toString());
                // TODO: Validate schema!
            } catch {
                throw new Error("Invalid JSON was provided!");
            }

            const keyFields = {
                event: transformationConfig.propertyColumns.event,
                timestamp: transformationConfig.propertyColumns.timestamp,
                distinct_id: transformationConfig.propertyColumns.distinctId,
                source: transformationConfig.dataSource
            };

            const replacements = transformationConfig.replacements;
            
            const event = row[keyFields.event.toUpperCase()] || row[keyFields.event.toLowerCase()];
            const unformattedTimestamp = row[keyFields.timestamp.toUpperCase()] || row[keyFields.timestamp.toLowerCase()];
            const raw_distinct_id = row[keyFields.distinct_id.toUpperCase()] || row[keyFields.distinct_id.toLowerCase()];

            let distinct_id = raw_distinct_id;
            
            if (!distinct_id) {
                distinct_id = transformationConfig.unmatchedUserDefault;
            }
            console.log(event, unformattedTimestamp, raw_distinct_id, distinct_id);

            const specialColumns = new Set([...Object.values(keyFields)]);
            
            const formattedRow: Record<string, any> = {};

            for (const key of Object.keys(row)) {
                const splitKey = key.split('_');

                for (let i = 0; i < splitKey.length; i++) {
                    /* If it's in the map, use it, otherwise stick to what it currently is;
                    Note: The title-casing happens before the replacement, so the replacement text 
                    stays as is! */
                    const originalSegment = splitKey[i]
                    splitKey[i] = toTitleCase(splitKey[i]);
                    splitKey[i] = replacements[originalSegment] || splitKey[i];
                }

                const newKey = key === keyFields.event ? key : splitKey.join(' ');

                if (!specialColumns.has(key)) {
                    formattedRow[newKey] = row[key];
                }
            }

            const properties = Object.keys(formattedRow).reduce((map, key) => {
                if (formattedRow[key] !== undefined && formattedRow[key] !== null) {
                    map[key] = formattedRow[key];
                }

                return map;
            }, {} as Record<string, any>);

            const timestamp = formatDateToIsoString(unformattedTimestamp);

            for (const property of Object.keys(properties)) {
                // Use some more robust logic for this
                if (property.toLowerCase().includes("timestamp")) {
                    properties[property] = formatDateToIsoString(properties[property]);
                }
            }

            const source = keyFields.source;
            
            return {
                event,
                properties: {
                    timestamp,
                    distinct_id: distinct_id,
                    source,
                    ...properties
                }
            }
        }
    }
}