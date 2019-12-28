const { PooledQldbDriver, QldbSession, createQldbWriter, QldbWriter, Result, TransactionExecutor, makePrettyWriter } = require('amazon-qldb-driver-nodejs');
const { getFieldValue, writeValueAsIon } = require('../helpers/ion');
// const {ionize} = require("ion-js");
// const {writeValueAsIon} = require("ion-js");
// const serviceConfigOptions = {
//     region: process.env.AWS_REGION,
//     sslEnabled: true,
// };
// const qldbDriver = new PooledQldbDriver(process.env.QLDB_NAME, serviceConfigOptions);

class qldbConnect {

    constructor(serviceConfigOptions) {
        this.qldbDriver = new PooledQldbDriver(process.env.QLDB_NAME, serviceConfigOptions);
    }

    async getTableNames() {
        return await this.qldbDriver.getSession().then((qldbSession) => {
            return qldbSession.getTableNames();
        });
    }

    async create(tableName,model) {
        return this.qldbDriver.getSession().then((qldbSession) => {
            return this.buildSqlInsert(tableName, model).then((sqlBuilder) => {
                return qldbSession.executeStatement(sqlBuilder.sql,[sqlBuilder.documentsWriter]).then((result) => {
                    if (result) {
                        return this.createObjectFromResults(result.getResultList(), true);
                    }
                    return false;
                })
            });
        });
    }

    async findOneBy(tableName,args) {
        return this.qldbDriver.getSession().then((qldbSession) => {
            return this.buildSqlSelect(tableName, args).then((sqlBuilder) => {
                return qldbSession.executeStatement(sqlBuilder.sql,[sqlBuilder.documentsWriter]).then((result) => {
                    if (result) {
                        return this.createObjectFromResults(result.getResultList(), true);
                    }
                    return false;
                })
            });
        });
    }

    async findBy(tableName,args) {
        return this.qldbDriver.getSession().then((qldbSession) => {
            return this.buildSqlSelect(tableName, args).then((sqlBuilder) => {
                return qldbSession.executeStatement(sqlBuilder.sql,[sqlBuilder.documentsWriter]).then((result) => {
                    if (result) {
                        return this.createObjectFromResults(result.getResultList());
                    }
                    return false;
                })
            });
        });
    }

    async buildSqlSelect(tableName, args) {
        // Build the SELECT part of the query
        let sqlSelectFields = '*';
        if (args.fields) {
            sqlSelectFields = args.fields.join(', ');
        }
        let sqlWhereFields = '';

        // Build the WHERE part and the ionize part for the query
        let documentsWriter = createQldbWriter();
        if (args.where) {
            for (const [key, value] of Object.entries(args.where)) {
                sqlWhereFields = sqlWhereFields + key + ' = ? AND ';
                writeValueAsIon(value, documentsWriter);
            }
            if (sqlWhereFields.length == 0) {
                sqlWhereFields = ' WHERE 1 = ?'; // << Functions as a "SELECT ALL"
                writeValueAsIon(1, documentsWriter);
            } else {
                sqlWhereFields = ' WHERE ' + sqlWhereFields.substr(0, sqlWhereFields.length - 4);
            }
        }
        return {
            sql:'SELECT ' + sqlSelectFields + ' FROM ' + tableName  + sqlWhereFields,
            documentsWriter: documentsWriter
        };
    }

    async buildSqlInsert(tableName, model) {
        let doc = {};
        let documentsWriter = createQldbWriter();
        for (const [fieldName, FieldOptions] of Object.entries(model)) {
            doc[fieldName] = FieldOptions.value;
        }
        writeValueAsIon(doc, documentsWriter);

        return {
            sql:'INSERT INTO ' + tableName  + ' ?',
            documentsWriter: documentsWriter
        };
    }

    async createObjectFromResults(results, singleResult = false) {
        let returnData = {};
        let n = 0;
        results.forEach(document => {
            document.next();
            document.stepIn();
            let recordData = {};
            for (var i = 0; i < 100; i++) { // <<<< Dirty hack to cycle over fields,
                document.next();
                let fieldName = document.fieldName();
                let fieldValue = null;
                if (fieldName == null) { // <<<< Break out of the for loop when the name equals "null" (part of dirty hack)
                    break;
                }
                switch (document.type().name) {
                    case 'string':
                        fieldValue = document.stringValue();
                        break;
                    case 'bool':
                        fieldValue = document.booleanValue();
                        break;
                    case 'int':
                        fieldValue = document.numberValue();
                        break;
                    case 'timestamp':
                        fieldValue = document.timestampValue();
                        break;
                }
                recordData[fieldName] = fieldValue;
            }
            returnData[n] = recordData;
            n++;
        });
        if (singleResult) {
            return returnData[0];
        }
        return returnData;
    }
}

// let qldbConnection = new qldbConnect();

module.exports = {
    qldbConnect,
}