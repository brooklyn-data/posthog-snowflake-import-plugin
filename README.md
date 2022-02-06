# Snowflake Import Plugin (BETA)

> Looking to contribute a transformation? See [Contributing a transformation](#contributing-a-transformation).

Import data from a Snowflake table in the form of PostHog events.

## ⚠️ Important Notice

This plugin is still in Beta! **Use it at your own risk**. Feel free to check out [its code](https://github.com/brooklyn-data/posthog-snowflake-import-plugin/blob/main/index.ts) and [submit feedback](https://github.com/brooklyn-data/posthog-snowflake-import-plugin/issues/new?title=Plugin+Feedback).

## Installation Instructions 

### 1. Select a Snowflake table to use for this plugin
### 2. Create a user with sufficient priviledges to read data from your table

We need to create a new table to store events and execute `INSERT` queries. You can and should block us from doing anything else on any other tables. Giving us table creation permissions should be enough to ensure this:

```sql
CREATE USER posthog WITH PASSWORD '123456yZ';
GRANT CREATE ON DATABASE your_database TO posthog;
```
### 3. Add the connection details at the plugin configuration step in PostHog

### 4. Determine what transformation to apply to your data

This plugin receives the data from your table and transforms it to create a PostHog-compatible event. To do this, you must select a transformation to apply to your data. If none of the transformations below suit your use case, feel free to contribute one via a PR to this repo.

#### Contributing a transformation

If none of the transformations listed below suits your use case, you're more than welcome to contribute your own transformation!

To do so, just add your transformation to the [`transformations` object](https://github.com/brooklyn-data/posthog-snowflake-import-plugin/blob/fe6e63c63a01241c1a3ef308c590b248dc657d8c/index.ts#L261) in the `index.ts` file and list it in the `plugin.json` [choices list](https://github.com/brooklyn-data/posthog-snowflake-import-plugin/blob/fe6e63c63a01241c1a3ef308c590b248dc657d8c/plugin.json#L54) for the field `transformationName`.

A transformation entry looks like this:

```js
'<transformation name here>': {
    author: '<your github username here>',
    transform: async (row, meta) => {
        /* 

        Fill in your transformation here and
        make sure to return an event according to 
        the TransformedPluginEvent interface:

        interface TransformedPluginEvent {
            event: string,
            properties?: PluginEvent['properties']
        }

        */
    }
}
```

Your GitHub username is important so that we only allow changes to transformations by the authors themselves.

Once you've submitted your PR, feel free to tag @yakkomajuri for review!

#### Available Transformations

##### default

The default transformation looks for the following columns in your table: `event`, `timestamp`, `distinct_id`, and `properties`, and maps them to the equivalent PostHog event fields of the same name.

**Code**

```js
async function transform (row, _) {
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
```

##### JSON Map

This transformation asks the user for a JSON file containing a map between their columns and fields of a PostHog event. For example:

```json
{
    "event_name": "event",
    "some_row": "timestamp",
    "some_other_row": "distinct_id"
}
```

**Code (Simplified\*)**

<small>*Simplified means error handling and type definitions were removed for the sake of brevity. See the full code in the [index.ts](/index.ts) file</small>

```js
async function transform (row, { attachments }) {            
    let rowToEventMap = JSON.parse(attachments.rowToEventMap.contents.toString())

    const eventToIngest = {
        event: '',
        properties: {}
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
```
