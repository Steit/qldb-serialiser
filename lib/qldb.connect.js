const { PooledQldbDriver, QldbSession, createQldbWriter, QldbWriter, Result, TransactionExecutor, makePrettyWriter } = require('amazon-qldb-driver-nodejs');
const { getFieldValue, writeValueAsIon } = require('../helpers/ion');
const  _ionJs = require("ion-js");

class qldbConnect {

    /**
     * Initiates the QLDB driver
     *
     * @param ledgerName
     * @param serviceConfigOptions
     */
    constructor(ledgerName, serviceConfigOptions) {
        this.qldbDriver = new PooledQldbDriver(ledgerName, serviceConfigOptions);
        this.tableNames = null;
        this.parseIon = this.parseIon.bind(this);
        this.getTableNames = this.getTableNames.bind(this);
    }

    /**
     * The tableNames function of the QLDB driver is called to test if the ledger is available, the credentials are okay
     * and general access to the tables is granted.
     *
     * @returns {Promise<null>}
     */
    async getTableNames() {
        if (this.tableNames != null) {
            return this.tableNames
        }
        this.tableNames = await this.qldbDriver.getSession().then((qldbSession) => {
            return qldbSession.getTableNames().then((result) => {
                if (result) {
                    return result.toString().split(",");
                }
                return null;
            })
        });
        return this.tableNames;

    }

    /**
     * Create a new record based on the model. The model holds the structure and all the data needed.
     *
     * @param tableName
     * @param model
     * @returns {Promise<Result>}
     */
    async create(tableName,model) {
        return this.qldbDriver.getSession().then((qldbSession) => {
            return this.buildSqlInsert(tableName, model).then((sqlBuilder) => {
                return qldbSession.executeStatement(sqlBuilder.sql,[sqlBuilder.documentsWriter]).then((result) => {
                    if (result) {
                        return this.createObjectFromResults(result.getResultList());
                    }
                    return false;
                })
            });
        });
    }

    /**
     * Finds one record by the supplied arguments. The param args consists of args.fields and args.where. The arg.fields variable
     * holds the names of the fields to be retrieved. The variable args.where holds the conditions that need to be met.
     *
     * @param tableName
     * @param model
     * @param args
     * @returns {Promise<Result>}
     */
    async findOneBy(tableName, model, args) {
        return this.qldbDriver.getSession().then((qldbSession) => {
            return this.buildSqlSelect(tableName, model, args).then((sqlBuilder) => {
                return qldbSession.executeStatement(sqlBuilder.sql,[sqlBuilder.documentsWriter]).then((result) => {
                    if (result) {
                        return this.createObjectFromResults(result.getResultList());
                    }
                    return false;
                })
            });
        });
    }

    /**
     * Find records by the supplied arguments. The param args consists of args.fields and args.where. The arg.fields variable
     * holds the names of the fields to be retrieved. The variable args.where holds the conditions that need to be met.
     *
     * @param tableName
     * @param model
     * @param args
     * @returns {Promise<Result>}
     */
    async findBy(tableName, model, args) {
        return this.qldbDriver.getSession().then((qldbSession) => {
            return this.buildSqlSelect(tableName, model, args).then((sqlBuilder) => {
                return qldbSession.executeStatement(sqlBuilder.sql,[sqlBuilder.documentsWriter]).then((result) => {
                    if (result) {
                        return this.createObjectFromResults(result.getResultList());
                    }
                    return false;
                })
            });
        });
    }

    /**
     * Builds the SQL query for selects. Based on the model Joins are created ans the selected fields are build. At the
     * moment it returns all the fields without filtering. The args.fields param is ignored for now.
     *
     * @param tableName
     * @param model
     * @param args
     * @returns {Promise<{documentsWriter: QldbWriter, sql: string}>}
     */
    async buildSqlSelect(tableName, model, args) {
        // Build the SELECT part of the query
        let sqlSelectFields = '*';
        if (args.fields) {
            sqlSelectFields = args.fields.join(', ');
        }
        const selectNames = await this.prepareNames(tableName, model);
        sqlSelectFields = selectNames.fieldnames.join(', ');

        let sqlWhereFields = '';
        let onStatement = '';
        if (selectNames.joinStatement.length > 0) {
            onStatement = selectNames.joinStatement.join(' ')
        }

        // Build the WHERE part and the ionize part for the query
        let documentsWriter = createQldbWriter();
        if (args.where) {
            for (const [key, value] of Object.entries(args.where)) {
                sqlWhereFields = sqlWhereFields + tableName + '.' + key + ' = ? AND ';
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
            sql:'SELECT ' + sqlSelectFields + ' FROM ' + tableName + onStatement  + sqlWhereFields,
            documentsWriter: documentsWriter
        };
    }

    /**
     * Builds the InsertSQL query. Straight forward insert as nested inserts are processed elsewhere.
     *
     * @param tableName
     * @param model
     * @returns {Promise<{documentsWriter: QldbWriter, sql: string}>}
     */
    async buildSqlInsert(tableName, model) {
        // FIXME Works but creates a timeout
        // if (!this.checkTableExistence(tableName)) {
        //     console.log('table not found and not created');
        // }
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

    /**
     * Checks if a table exists and if not creates that table. The function works but slows down the QLDB connection
     * resulting in timeouts in further calls.
     *
     * @param tableName
     * @returns {Promise<boolean|Result>}
     */
    async checkTableExistence(tableName) {
        await this.getTableNames();
        if (this.tableNames.indexOf(tableName) == -1) {
            return this.qldbDriver.getSession().then((qldbSession) => {
                return qldbSession.executeStatement('CREATE TABLE ' + tableName).then((result) => {
                    if (result) {
                        console.log('table created');
                        return true;
                    }
                    console.log('table not created');
                    return false;
                })
            });
        }
        console.log('table '+tableName+'  found');
        return true;
    }

    /**
     * Create the names needed for the SQL query. Field names are prepended with the table name. Optional JOIN statements
     * are created and joined based on the documentId in the metadata of the document. The model contains all the needed
     * information to create these SQL statement parts.
     *
     * @param tableName
     * @param model
     * @returns {Promise<{fieldnames: [], joinStatement: []}>}
     */
    async prepareNames(tableName, model) {
        let fieldNames = [];
        let joinStatement = [];
        for (const [fieldName, FieldOptions] of Object.entries(model)) {
            //Check if any of the requested fields is a nested one (DataType:LEDGER)
            if ((FieldOptions.type) && (FieldOptions.type.name.toLowerCase() == 'ledger')) {
                joinStatement.push(' JOIN _ql_committed_' + FieldOptions.model.tableName + ' ON ' + tableName + '.' + fieldName + '=' + '_ql_committed_'+FieldOptions.model.tableName + '.metadata.id');
                fieldNames.push('_ql_committed_'+FieldOptions.model.tableName + '.data AS ' + fieldName);
            } else {
                fieldNames.push(tableName + '.' + fieldName);
            }
        };
        return {
            fieldnames: fieldNames,
            joinStatement: joinStatement
        };
    }

    /**
     * Creates a human readable object from the results that the QLDB returns. The results is in a 'getResultList' format
     *
     * @param results
     * @returns {*}
     */
    createObjectFromResults(results) {
        const returnObject = results.map(this.parseIon);
        if (returnObject.length == 1) {
            return returnObject[0];
        }
        return returnObject;
    }

    /**
     * Parses the ion document(s) created by the QLDB. The IonTypes.STRUCT and IonTypes.LIST types have a recurrent call
     * to this function.
     *
     * @param ion
     * @returns {{}|null|[]}
     */
    parseIon(ion) {
        const structToReturn = {};

        if (ion.type() === null) {
            ion.next();
        }

        let fieldValue = null;
        switch (ion.type()) {
            case _ionJs.IonTypes.STRING:
                fieldValue = ion.stringValue();
                break;
            case _ionJs.IonTypes.BOOL:
                fieldValue = ion.booleanValue();
                break;
            case _ionJs.IonTypes.INT:
                fieldValue = ion.numberValue();
                break;
            case _ionJs.IonTypes.TIMESTAMP:
                fieldValue = ion.timestampValue().toString();
                break;
            case _ionJs.IonTypes.STRUCT:
                let type;
                const currentDepth = ion.depth();
                ion.stepIn();

                while (ion.depth() > currentDepth) {
                    type = ion.next();
                    if (type === null) {
                        ion.stepOut();
                    } else {
                        structToReturn[ion.fieldName()] = this.parseIon(ion);
                    }
                }
                return structToReturn;
                break;
            case _ionJs.IonTypes.LIST:
                const list = [];
                ion.stepIn();

                while (ion.next() != null) {
                    const itemInList = this.parseIon(ion);
                    list.push(itemInList);
                }
                return list;
                break;
        }
        return fieldValue;
    }

}

module.exports = {
    qldbConnect,
}