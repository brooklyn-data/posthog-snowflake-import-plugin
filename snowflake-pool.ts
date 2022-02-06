/*
    Not currently used.
    This can be used once we can modularize `index.ts` into separate files
*/
import * as snowflake from 'snowflake-sdk';

import { createPool, Pool } from 'generic-pool';

interface SnowflakeOptions {
    account: string
    username: string
    password: string
    database?: string
    schema?: string
    table?: string
    warehouse?: string
    role?: string
}

export class SnowflakeConnectionPool {
    private pool: Pool<snowflake.Connection>
    private database?: string
    private schema?: string
    // warehouse: string // Might not be needed if default is set

    constructor({
        account,
        username,
        password,
        database,
        schema,
        role
    }: SnowflakeOptions) {
        this.pool = this.createConnectionPool(account, username, password, /*warehouse,*/ role);
        this.database = database?.toUpperCase();
        this.schema = schema?.toUpperCase();
        // this.warehouse = warehouse.toUpperCase();
    }

    private createConnectionPool(
        account: string,
        username: string,
        password: string,
        // warehouse: string,
        role?: string
    ): SnowflakeConnectionPool['pool'] {
        const roleConfig = role ? { role } : {}
        return createPool(
            {
                create: async () => {
                    const connection = snowflake.createConnection({
                        account,
                        username,
                        password,
                        database: this.database,
                        schema: this.schema,
                        // warehouse
                        ...roleConfig,
                    })

                    await new Promise<string>((resolve, reject) =>
                        connection.connect((err, conn) => {
                            if (err) {
                                console.error('Error connecting to Snowflake: ' + err.message)
                                reject(err)
                            } else {
                                resolve(conn.getId())
                            }
                        })
                    )

                    return connection
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
                    complete: (err, stmt, rows) => {
                        if (err) {
                            console.error('Error executing Snowflake query: ', { sqlText, error: err.message })
                            return reject(err)
                        }

                        const columns = stmt.getColumns();

                        let result: Record<string, any>[] = [];

                        if (rows && rows.length) {
                            result = rows.map(row => columns.reduce((currentMap, column, columnIndex): Record<string, any> => {
                                    currentMap[column.getName()] = row[columnIndex];
                                    return currentMap;
                                }, {} as Record<string, any>)
                            )
                        }
                        return resolve(result)
                    },
                })
            )
        } finally {
            await this.pool.release(snowflakeConnection)
        }
    }

}