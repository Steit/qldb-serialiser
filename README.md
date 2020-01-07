# QLDB Serialise

The QLDB Serialiser is a promise-based Node.js ORM for the AWS QLDB Ledger. It is founded on the idea and partially the structure behind Sequelize. For the moment it is using the beta version of the AWS Node.js drivers.

See [AWS Qldb node.js driver](https://www.npmjs.com/package/amazon-qldb-driver-nodejs) for more on this. As with the driver it uses I need to stronlgly mention that this is a preview release. For myself it is a testcase connector to see how to integrate QLDB with other applications.

## Installation

```bash
$ npm install --save qldb-serialiser # Only master is available right now
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
let qldb = new qldbConnect(qldbSettings);
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

const Asset = new Ledger(qldb, 'Assets', {
        assetId: {
            type: DataTypes.STRING,
            allowNull: false,
            primaryKey: true,
        },
        assetData: {
            type: DataTypes.OBJECT,
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
            type: DataTypes.STRING,
            allowNull: false,
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