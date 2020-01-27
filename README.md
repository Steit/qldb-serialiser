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
 ```javascript
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
## Changes
**version 1.0.0**
* Added complex models
* Added DataTypes.OBJECT and DataTypes.ARRAY
* Refactoring of SQL Insert creation

**version 0.0.3-beta**
* Nested models
* Added DataTpes.LEDGER
* Cleaner ion to object creation
* Updated dependencies
