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
        this.create = this.create.bind(this);
        this.buildDoc = this.buildDoc.bind(this);
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
     * Get data from the committed table data. The tablename is automatically prepended with the '_ql_committed_'. The args
     * have a similar build as the regular findBy and findOneBy functions.
     *
     * @param tableName
     * @param args
     * @returns {Promise<Result>}
     */
    async findCommittedData(tableName, args) {
        return this.qldbDriver.getSession().then((qldbSession) => {
            return this.buildMetaSqlSelect(tableName, args).then((sqlBuilder) => {
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
     * moment the args.fields param is ignored for now.
     *
     * @param tableName
     * @param model
     * @param args object with a .where that holds the search/filter information and a .fields array that folds the fields
     *      that should be returned.
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

        // let sqlWhereFields = '';
        let onStatement = '';
        if (selectNames.joinStatement.length > 0) {
            onStatement = selectNames.joinStatement.join(' ')
        }

        // Build the WHERE part and the ionize part for the query
        let sqlWhereValues = createQldbWriter();
        let sqlWhereFields = '';
        if (args.where) {
            for (const [key, value] of Object.entries(args.where)) {
                sqlWhereFields = sqlWhereFields + tableName + '.' + key + ' = ? AND ';
                writeValueAsIon(value, sqlWhereValues);
            }
            if (sqlWhereFields.length == 0) {
                sqlWhereFields = ' WHERE 1 = ?'; // << Functions as a "SELECT ALL"
                writeValueAsIon(1, sqlWhereValues);
            } else {
                sqlWhereFields = ' WHERE ' + sqlWhereFields.substr(0, sqlWhereFields.length - 4);
            }
        }

        return {
            sql:'SELECT ' + sqlSelectFields + ' FROM ' + tableName + onStatement  + sqlWhereFields,
            documentsWriter: sqlWhereValues
        };
    }

    /**
     * Builds the SQL for the select on the committed data in the QLDB.
     *
     * @param tableName
     * @param args
     * @returns {Promise<{documentsWriter: QldbWriter, sql: string}>}
     */
    async buildMetaSqlSelect(tableName, args) {
        let sqlWhereValues = createQldbWriter();
        let sqlWhereFields = '';
        if (args.where) {
            for (const [key, value] of Object.entries(args.where)) {
                sqlWhereFields = sqlWhereFields + key + ' = ? AND ';
                writeValueAsIon(value, sqlWhereValues);
            }
            if (sqlWhereFields.length == 0) {
                sqlWhereFields = ' WHERE 1 = ?'; // << Functions as a "SELECT ALL"
                writeValueAsIon(1, sqlWhereValues);
            } else {
                sqlWhereFields = ' WHERE ' + sqlWhereFields.substr(0, sqlWhereFields.length - 4);
            }
        }
        return {
            sql:'SELECT * FROM _ql_committed_' + tableName  + sqlWhereFields,
            documentsWriter: sqlWhereValues
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
        // let doc = {};
        const doc = await this.buildDoc(model);

        let documentsWriter = createQldbWriter();
        writeValueAsIon(doc, documentsWriter);

        return {
            sql:'INSERT INTO ' + tableName  + ' ?',
            documentsWriter: documentsWriter
        };
    }

    /**
     * Mainly used by the buildSqlInsert function to build the query. Nested/linked tables defined as LEDGER in the model
     * are inserted if they are new. Existing documentId's of referenced tables are already replaced from the entered data
     * in the model when the format and data is being checked. The data is not cheked again in this function and relies
     * on the model to do that in the mapDataToModel function.
     *
     * @param model
     * @param data
     * @returns {Promise<{}>}
     */
    async buildDoc(model, data = false) {
        let doc = {};
        for (const [fieldName, fieldOptions] of Object.entries(model)) {
            let fieldType = fieldOptions.type.name.toLowerCase()
            if (!data) {
                data = model[fieldName].value;
            }
            if (fieldType == 'ledger') { // Check the values in the linked model
                if (typeof  model[fieldName].value != 'object') {
                    const result = await fieldOptions.model.getByPk(data[fieldName]);
                    doc[fieldName] = model[fieldName].value;
                } else {
                    let result = await this.create(fieldOptions.model.tableName, fieldOptions.model.model);
                    doc[fieldName] = result.documentId;
                }
            } else if (fieldType == 'object') {
                doc[fieldName] = await this.buildDoc(fieldOptions.model, data);
            } else if (Array.isArray(model[fieldName].value)) {
                let arrayValue = [];
                for (const element of model[fieldName].value) {
                    // Set the values for each entry in the array
                    for (const [key, value] of Object.entries(fieldOptions.model)) {
                        fieldOptions.model[key].value = element[key];
                    }
                    let result = await this.buildDoc(fieldOptions.model, element);
                    arrayValue.push(result);
                    result = null;
                }
                doc[fieldName] = arrayValue;
            } else {
                if (model[fieldName].value != null) {
                    doc[fieldName] = model[fieldName].value;
                }
            }
        };
        return doc;
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
                        return true;
                    }
                    return false;
                })
            });
        }
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
        for (const [fieldName, fieldOptions] of Object.entries(model)) {
            //Check if any of the requested fields is a nested one (DataType:LEDGER)
            if ((fieldOptions.type) && (fieldOptions.type.name.toLowerCase() == 'ledger')) {
                joinStatement.push(' JOIN _ql_committed_' + fieldOptions.model.tableName + ' ON ' + tableName + '.' + fieldName + '=' + '_ql_committed_'+fieldOptions.model.tableName + '.metadata.id');
                fieldNames.push('_ql_committed_'+fieldOptions.model.tableName + '.data AS ' + fieldName);
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