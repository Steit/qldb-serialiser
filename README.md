# QLDB Serialise

The QLDB Serialiser is a promise-based Node.js ORM for the AWS QLDB Ledger. It is founded on the idea and partially the structure behind Sequelize. For the moment it is using the beta version of the AWS Node.js drivers.

See [AWS Qldb node.js driver](https://www.npmjs.com/package/amazon-qldb-driver-nodejs) for more on this. As with the driver it uses I need to stronlgly mention that this is a preview release. For myself it is a testcase connector to see how to integrate QLDB with other applications.

## Installation

```bash
$ npm install qldb-serialiser
```
or clone this project into your node_modules folder.
## Usage
To use the QLDB Serialser in your code create a connector and ue this connector in your model

Connector (/config/qldb.connect.js)
```javascript
const {qldbConnect, Ledger, DataTypes} = require('qldb-serialiser');

let qldbSettings = {
    "region": process.env.AWS_REGION,
    "sslEnabled": true,
};
let qldb = new qldbConnect(process.env.QLDB_NAME, qldbSettings);
qldb.getTableNames()
    .then(() => {
        console.log("Database has been connected")
    })
    .catch((err) => {
        console.log("Unable to connect to Database");
    });

module.exports = {
    qldb,
    Ledger,
    DataTypes
}
```

Model example (/models/asset.model.js)
```javascript
const {qldb, Ledger, DataTypes} = require('../config/qldb.connect');

const AssetData = require('./asset_data.model');

const Asset = new Ledger(qldb, 'Assets', {
        assetId: {
            type: DataTypes.STRING,
            allowNull: false,
            primaryKey: true,
        },
        assetData: {
            type: DataTypes.LEDGER,
            model: AssetData,
            allowNull: true,
        },
        title: {
            type: DataTypes.STRING,
            allowNull: false,
        },
        description: {
            type: DataTypes.STRING,
            allowNull: false,
        },
        owner: {
            type: DataTypes.OBJECT,
            allowNull: false,
            model: {
                individual: {
                    type: DataTypes.ARRAY,
                    model: {
                        person: {
                            type: DataTypes.LEDGER,
                            model: Person,
                            allowNull: false
                        },
                        percentage: {
                            type: DataTypes.NUMBER,
                            allowNull: false
                        }
                    },
                    allowNull: false,
                },
            }
        },
        price: {
            type: DataTypes.NUMBER,
            allowNull: false,
        },
        hideOwner: {
            type: DataTypes.BOOL,
            allowNull: false,
            default: false
        },
        hidePrice: {
            type: DataTypes.BOOL,
            allowNull: false,
            default: false
        }
    },
    {
        timestamps: true
    });

module.exports = Asset;
```
Note that in the above sample nested models are made possible with the usage of the DataTypes.LEDGER. 

When inserting data into the LEDGER type standard JSON is expected. All fieldnames are tested in the same way as any other model.
 ```json
{
	"assetData":{
        "id": "....",
		"name": "....",
		"description": "...."
	},
	"title":"....",
	"description":"...."
}
```
## Using records
The next samples show how to interact with the models to manipulate the data in the QLDB. The next few samples use the Asset model as described above.
The sample functions are indicative. 
### Reading a record
Reads can be done in serveral ways. There are three functions; `'getAll()'`, `'getBy()'` and `'getByPk()'`. The `'getByPk()'` function is a wrapper around the `'getBy()'` function thet finds the Primary key of the model and based on that creates the where clause needed by the `'getBy()'` function.
````javascript
    async getAssetById(assetId){
        let assetResult = await Asset.getByPk(assetId);
        if(assetResult) return assetResult;
        return false
    }
```` 
Beware of using the `'getAll()`' function as QLDB at the moment does not support paginated results and will get all the records. This may end up in costly queries being run for a long time.

### Adding or inserting record
The adding of a record is done by passing the JSON containing all fields to the add function in the model. An example of the JSON format can be seen above.

````javascript
    async createAsset(asset){
        let assetResult = await Asset.add(asset);
        if(assetResult) return assetResult;
        return false
    }
````

### Updating a record
To update a record simply pass the updated fields in a JSON model to the model. The form of the whereClause is the same as for a search action '{ fieldname: fieldValue }'. 
```javascript
    async updateAsset(whereClause, updates){
        let assetResult = await Asset.update({fields: updates, where: whereClause})
        if(assetResult) return assetResult;
        return false

    }
```

## Changes
**version 1.1.4**
* Added 'IN' selector to where selection function.

**version 1.1.3**
* Bugfix on the nested array update.

**version 1.1.2**
* Added nested updates for nested ledgers 

**version 1.1.1**
* Added getHistory & getHistoryByPk functions to retrieve all history for a record
* Impoved Query generation
* Updated to use amazon-qldb-driver-nodejs v. 0.1.2-preview.1
* Changed behaviour of nested arrays to enable an array of linked ledger models

**version 1.1.0**
* Added update functionality.
* Added record documentation.
* Removed the logutil and ion TS helpers
* Moved the ionparser to separate helper
* More efficient use of the 'amazon-qldb-driver-nodejs' driver


**version 1.0.2**
* Modified LEDGER portion in base model mapping to allow references to existing documents

**version 1.0.0**
* Added complex models
* Added DataTypes.OBJECT and DataTypes.ARRAY
* Refactoring of SQL Insert creation

**version 0.0.3-beta**
* Nested models
* Added DataTpes.LEDGER
* Cleaner ion to object creation
* Updated dependencies
