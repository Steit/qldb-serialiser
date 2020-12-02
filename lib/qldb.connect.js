const { Agent } = require('https');
const { QldbDriver, RetryConfig  } = require('amazon-qldb-driver-nodejs');

const {Operators} = require('./qldb.operators');
const { arrayStringify } = require('./utils');

class qldbConnect {
    /**
     * Initiates the QLDB driver
     *
     * @param ledgerName
     * @param serviceConfigOptions
     */
    constructor(ledgerName, serviceConfigOptions) {
        const { retryLimit, maxConcurrentTransactions, timeoutMillis, ...qldbClientOptions } = serviceConfigOptions

        //Reuse connections with keepAlive
        const agentForQldb = new Agent({
            keepAlive: true,
            maxSockets: maxConcurrentTransactions
        });

        qldbClientOptions.httpOptions = {
            agent: agentForQldb
        };

        const retryConfig = new RetryConfig(retryLimit);
        this.qldbDriver = new QldbDriver(ledgerName, qldbClientOptions, maxConcurrentTransactions, retryConfig);

        this.qldbSession = null;
        this.tableNames = null;
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
        this.tableNames = await this.qldbDriver.getTableNames().then((result) => {
            if (result) {
                return result.toString().split(",");
            }
            return null;
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
        try {
            return this.buildSqlInsert(tableName, model).then((sqlBuilder) => {
                return this.executeStatement(sqlBuilder).then((result) => {
                    if (result) {
                        return this.createObjectFromResults(result);
                    }
                    return false;
                })
            });
        } catch (err) {
            throw new Error('Create record failed', err);
        } finally {
            await this.closeSession()
        }

    }

    /**
     * Update an existing record with the data in the model
     *
     * @param tableName
     * @param model
     * @returns {Promise<Result>}
     */
    async update(tableName, args, model) {
        return this.buildSqlUpdate(tableName, args, model).then((sqlBuilder) => {
            return this.executeStatement(sqlBuilder).then((result) => {
                if (result) {
                    return this.createObjectFromResults(result);
                }
                return false;
            })
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
        return this.buildSqlSelect(tableName, model, args).then((sqlBuilder) => {
            return this.executeStatement(sqlBuilder).then((result) => {
                if ((result) && (result.length > 0)){
                    return this.createObjectFromResults(result, true);
                }
                return false;
            })
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
        return this.buildSqlSelect(tableName, model, args).then((sqlBuilder) => {
            return this.executeStatement(sqlBuilder).then((result) => {
                if (result) {
                    let objectResult = this.createObjectFromResults(result);
                    if (args.order) {
                        objectResult = this.fakeOrdering(objectResult, args);
                    }
                    if (args.limit) {
                        objectResult = this.fakePagination(objectResult, args);
                    }
                    return objectResult;
                }
                return false;
            })
        });
    }

    /**
     * Delete a record from the QLDB ledger. The option {recursive: true} in the arguments will delete linked entries
     * that are defined in the model as LEDGER.
     *
     * @param tableName
     * @param model
     * @param args
     * @returns {Promise<Reader[]>}
     */
    async delete(tableName, model, args) {
        return this.buildSqlDelete(tableName, model, args).then((sqlBuilder) => {
            if (sqlBuilder == false) {
                return 'unsafe delete with empty where statement.';
            }
            let results = [];
            sqlBuilder.split(';').forEach(sqlStatement => {
                if (sqlStatement.length == 0) {
                    return;
                }
                let result = this.executeStatement(sqlStatement).then((result) => {
                    return result;
                })
                results.push(result);
            })
            return results;
        });
    }

    /**
     * Get data from the committed table data. The tableName is automatically prepended with the '_ql_committed_'. The args
     * have a similar build as the regular findBy and findOneBy functions.
     *
     * @param tableName
     * @param args
     * @returns {Promise<Result>}
     */
    async findCommittedData(tableName, args) {
        return this.buildMetaSqlSelect(tableName, args).then((sqlBuilder) => {
            return this.executeStatement(sqlBuilder).then((result) => {
                if (result) {
                    return this.createObjectFromResults(result);
                }
                return false;
            })
        });
    }

    /**
     * Get the historic data and changes for any document. This function is called by getHistoryByDocumentId and
     * getHistoryByPk. The args section defines if the query is fun on the metadat or on the data of the history.
     *   const args = {
     *     where: whereArgs,
     *     useMetaData: false,
     *     startDate: startDate,
     *     endDate: endDate
     *   }
     * Where the whereArgs are {<primary_key_name>: id} or {id: <document_id>>}
     * The value useMetaData indicates if the search is done in the data or metaData fields
     *
     * @param tableName
     * @param args
     * @returns {Promise<Reader[]>}
     */
    async getHistory(tableName, args ) {
        return this.buildHistorySqlSelect(tableName, args).then((sqlBuilder) => {
            return this.executeStatement(sqlBuilder).then((result) => {
                if (result) {
                    return this.createObjectFromResults(result);
                }
                return false;
            })
        });
    }

    /**
     * Builds the SQL query for selects. Based on the model Joins are created and the selected fields are build.
     *
     * @param tableName
     * @param model
     * @param args object with a .where that holds the search/filter information and a .fields array that folds the fields
     *      that should be returned.
     * @returns string}
     */
    async buildSqlSelect(tableName, model, args) {
        // Build the SELECT part of the query
        const selectNames = await this.prepareNames(tableName, model, (args.fields) ? args.fields : null);
        const sqlSelectFields = selectNames.fieldNames.join(', ');

        let onStatement = '';
        if (selectNames.joinStatement.length > 0) {
            onStatement = selectNames.joinStatement.join(' ')
        }
        // Get the WHERE part for the query
        const sqlWhere = await this.createSqlWhere(args, tableName, model);
        return 'SELECT ' + sqlSelectFields + ' FROM ' + tableName + onStatement  + sqlWhere +';';
    }

    /**
     * Build the UPDATE part of the query
     *
     * @param tableName
     * @param args object with a .where that holds the filter conditions and a .fields array that folds the fields
     *      that should be updated. There is no check if the updated data if is different from the existing one.
     * @returns string}
     */
    async buildSqlUpdateRecursive(tableName, args, model) {
        let sqlUpdate = '';
        let sqlLinked = '';
        for (const [fieldName, fieldValue] of Object.entries(args.fields)) {
            if (!model[fieldName]) {
                throw new Error(`${fieldName} is not defined for ${tableName}`);
            }
            // Test if the update field is a linked ledger
            let fieldType = model[fieldName].type.name.toLowerCase()
            if (fieldType == 'ledger') {
                const pkValue = await this.getLinkedLedgerPkValue(tableName, fieldName, args.where);
                const pkName = model[fieldName].model.primaryKey;
                const subArgs = {
                    fields: fieldValue,
                    where: {}
                }
                subArgs.where[pkName] = pkValue;
                sqlLinked = await this.buildSqlUpdate(model[fieldName].model.tableName, subArgs, model[fieldName].model.model);
                this.executeRawSQLAndForget(sqlLinked);
            } else if ((Array.isArray(model[fieldName].value)) || (fieldType == 'json')) {
                const newValue = arrayStringify(fieldValue);
                sqlUpdate = sqlUpdate + tableName + "." + fieldName + " = " + newValue + " , ";
            } else if ((fieldType == 'number') || (fieldType == 'int')) {
                sqlUpdate = sqlUpdate + tableName + "." + fieldName + " = " + fieldValue + " , ";
            } else if (fieldType == 'object') {
                const currentLayer = tableName + "." + fieldName;
                const sqlRecursive = await this.buildSqlUpdateRecursive(currentLayer, {fields: fieldValue}, model[fieldName].model);
                sqlUpdate = sqlUpdate + sqlRecursive;
            } else {
                sqlUpdate = sqlUpdate + tableName + "." + fieldName + " = '" + fieldValue + "' , ";
            }
        }
        return sqlUpdate
    }

    /**
     * Builds the SQL query for updates. Based on the model, no nested updates supported.
     *
     * @param tableName
     * @param args object with a .where that holds the filter conditions and a .fields array that folds the fields
     *      that should be updated. There is no check if the updated data if is different from the existing one.
     * @returns string}
     */
    async buildSqlUpdate(tableName, args, model) {
        const sqlUpdate = await this.buildSqlUpdateRecursive(tableName, args, model);
        // Create the WHERE condition for the update
        const sqlWhere = await this.createSqlWhere(args, tableName, model);
        return 'UPDATE ' + tableName + ' SET ' + sqlUpdate.substr(0, sqlUpdate.length - 2)  + sqlWhere +";";
    }

    /**
     * Builds the SQL for the deletion of records.
     *
     * @param tableName
     * @param args
     * @param model
     * @returns {Promise<string>}
     */
    async buildSqlDelete(tableName, model, args) {
        const recursive = (args.recursive === true) ? true : false;
        // Get the WHERE part for the query
        const sqlWhere = await this.createSqlWhere(args, tableName, model);
        if (sqlWhere.length == 0) {
            return false;
        }
        let deleteSql = 'DELETE FROM ' + tableName + sqlWhere + ';'
        if (recursive === false) {
            return deleteSql;
        }
        for (const [fieldName, fieldValue] of Object.entries(model)) {
            // Test if the update field is a linked ledger
            let fieldType = model[fieldName].type.name.toLowerCase();
            if (fieldType == 'ledger') {
                // Get the PK-name and PK-value of the linked ledger, use the where args only from the original call.
                const pkValue = await this.getLinkedLedgerPkValue(tableName, fieldName, args.where);
                const pkName = model[fieldName].model.primaryKey;
                let subArgs = {
                    recursive: recursive,
                    where: []
                };
                subArgs.where[pkName] = pkValue;
                const subSQL = await this.buildSqlDelete(model[fieldName].model.tableName, model[fieldName].model.model, subArgs);
                deleteSql += subSQL;
            }
        }
        return deleteSql;
    }

    async getLinkedLedgerPkValue(tableName, pkFieldName, args) {
        let sqlWhere = '';
        for (const [fieldName, fieldValue] of Object.entries(args)) {
            sqlWhere += fieldName + '= \''+ fieldValue +'\' AND ';
        }
        let sql = 'SELECT '+ pkFieldName +' FROM '+ tableName +' WHERE '+ sqlWhere.substr(0, sqlWhere.length - 4) +' ;';
        const linkedLedger = await this.executeRawSQL(sql);
        return linkedLedger[0][pkFieldName];
    }

    async executeRawSQL(sql) {
        return this.executeStatement(sql).then((result) => {
            if (result) {
                return this.createObjectFromResults(result);
            }
            return false;
        })
    }

    async executeRawSQLAndForget(sql) {
        return this.executeStatement(sql).then((result) => {
            return true;
        })
    }
    /**
     * Build the WHERE part and the ionize part for the query
     *
     * @param args
     * @param tableName
     * @param model
     * @returns {string}
     */
    createSqlWhere (args, tableName, model) {
        // Build the WHERE part and the ionize part for the query
        let sqlWhere = '';
        const operatorKeys = Operators.getOperatorNames();
        if (args.where) {
            // Loop over args to see if there are keys that are a ledger and if so combine the table and field(s) name for that key.
            for (let [key, value] of Object.entries(args.where)) {
                if ((model[key].type.name.toLowerCase() == 'ledger') && (typeof(value) == 'object')) {
                    for (let [subKey, subValue] of Object.entries(value)) {
                        args.where[(model[key].model.tableName + '.' + subKey).toString()] = subValue;
                    }
                } else {
                    args.where[(tableName + '.' + key).toString()] = value;
                }
                delete args.where[key];
            }
            for (let [key, value] of Object.entries(args.where)) {
                let operator = Operators.EQ;
                if ((typeof(value) == 'array') || (typeof(value) == 'object')) {
                    if ((typeof(value[0]) == 'object') && (operatorKeys.indexOf(value[0].name.toUpperCase()) >= 0)) {
                        operator = Operators[value[0].name.toUpperCase()];
                        value = value[1];
                    } else {
                        operator = Operators.IN;
                    }
                }
                if ((typeof(value) == 'array') || (typeof(value) == 'object')) {
                    let sendValues = [];
                    value.forEach(item =>{
                        if (typeof(item) == 'string') {
                            sendValues.push("'"+item+"'");
                        } else {
                            sendValues.push(item);
                        }
                    });
                    sqlWhere = sqlWhere + key + operator.operator + "[" + sendValues.join(',') + "] AND ";
                } else if(typeof(value) === 'number') {
                    sqlWhere = sqlWhere + key + operator.operator + value + " AND ";
                } else {
                    sqlWhere = sqlWhere + key + operator.operator + "'" + value + "' AND ";
                }
            }
            if (sqlWhere.length == 0) {
                sqlWhere = ' WHERE 1 = 1'; // << Functions as a "SELECT ALL"
            } else {
                sqlWhere = ' WHERE ' + sqlWhere.substr(0, sqlWhere.length - 4);
            }
        }
        return sqlWhere;
    }

    /**
     * NOTE: Not to be used, ORDER BY is not yet supported by PartiQL
     * create the 'ORDER BY' part of the query.
     *
     * @param args
     */
    createSqlOrder (args, tableName) {
        let sqlOrder = '';
        const operatorKeys = Operators.getOperatorNames();
        if (args.order) {
            for (let [key, value] of Object.entries(args.order)) {
                sqlOrder += tableName + "." + key + ' ' + value + ', '
            }
            sqlOrder = ' ORDER BY ' + sqlOrder.substr(0, sqlOrder.length - 2);
        }
        return sqlOrder;
    }

    /**
     * Builds the SQL for the select on the committed data in the QLDB.
     *
     * @param tableName
     * @param args
     * @returns string
     */
    async buildMetaSqlSelect(tableName, args) {
        let sqlWhere = '';
        if (args.where) {
            for (const [key, value] of Object.entries(args.where)) {
                sqlWhere = sqlWhere + key + " = '"+ value +"' AND ";
            }
            if (sqlWhere.length == 0) {
                sqlWhere = ' WHERE 1 = ?'; // << Functions as a "SELECT ALL"
            } else {
                sqlWhere = ' WHERE ' + sqlWhere.substr(0, sqlWhere.length - 4);
            }
        }
        return 'SELECT * FROM _ql_committed_' + tableName  + sqlWhere + ';';
    }

    /**
     * Builds the SQL for the select on the historic data in the QLDB.
     *
     * @param tableName
     * @param args
     * @returns string
     */
    async buildHistorySqlSelect(tableName, args) {
        let sqlWhere = '';
        if (args.where) {
            let fieldPrefix = 'h.data.'
            if (args.useMetaData) {
                fieldPrefix = 'h.metadata.'
            }
            for (const [key, value] of Object.entries(args.where)) {
                sqlWhere = sqlWhere + fieldPrefix + key + " = '"+ value +"' AND ";
            }
            if (sqlWhere.length == 0) {
                sqlWhere = ' '; // << Functions as a "SELECT ALL"
            } else {
                sqlWhere = ' WHERE ' + sqlWhere.substr(0, sqlWhere.length - 4);
            }
        }
        let fromSection = tableName;
        if (args.startDate) {
            fromSection += ', `' + args.startDate +'T`';
        }
        if (args.endDate) {
            fromSection += ', `' + args.endDate +'T`';
        }
        return 'SELECT * FROM history( '+ fromSection +') AS h' + sqlWhere + ';';
    }

    /**
     * Builds the InsertSQL query. Straight forward insert as nested inserts are processed elsewhere.
     *
     * @param tableName
     * @param model
     * @returns {Promise  string}
     */
    async buildSqlInsert(tableName, model) {
        // FIXME Works but creates a timeout
        // if (!this.checkTableExistence(tableName)) {
        //     console.log('table not found and not created');
        // }
        const doc = await this.buildDoc(model);
        const insertValues = JSON.stringify(doc).replace(/"/ig, "'")

        return 'INSERT INTO ' + tableName  + ' VALUE ' + insertValues ;
    }

    /**
     * Mainly used by the buildSqlInsert function to build the query. Nested/linked tables defined as LEDGER in the model
     * are inserted if they are new. Existing documentId's of referenced tables are already replaced from the entered data
     * in the model when the format and data is being checked. The data is not checked again in this function and relies
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
            if (fieldType == 'ledger') { // process the values in the linked model
                doc[fieldName] = await this.processLedgerData(fieldOptions.model, model[fieldName].value);
            } else if (fieldType == 'object') {
                doc[fieldName] = await this.buildDoc(fieldOptions.model, data);
            } else if (Array.isArray(model[fieldName].value)) {
                let arrayValue = [];
                for (const element of model[fieldName].value) {
                    // Set the values for each entry in the array
                    let model = null;
                    let result = null;
                    // Enable an array of linked ledger models
                    if ((fieldOptions.model) && (fieldOptions.model.constructor) && (fieldOptions.model.constructor.name.toLowerCase() == 'ledger')) {
                        for (const [key, value] of Object.entries(fieldOptions.model.model)) {
                            if (typeof(element[key]) != 'undefined') {
                                fieldOptions.model.model[key].value = element[key];
                            }
                        }
                        result = await this.processLedgerData(fieldOptions.model,  element);
                    } else if (fieldOptions.model) {
                        for (const [key, value] of Object.entries(fieldOptions.model)) {
                            fieldOptions.model[key].value = element[key];
                        }
                        result = await this.buildDoc(fieldOptions.model, element);
                    } else {
                        result = element;
                    }

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
     * Process the ledger type elements in the buildDoc process
     *
     * @param ledgerModel
     * @param value
     * @returns {Promise<*>}
     */
    async processLedgerData(ledgerModel, value) {
        if (typeof(value) != 'object') {
            return value;
        }
        const result = await this.create(ledgerModel.tableName, ledgerModel.model);
        return ledgerModel.model[ledgerModel.primaryKey].value;
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
            await this.qldbDriver.executeLambda(async (txn) => {
                return await txn.execute('CREATE TABLE ' + tableName).then((result) => {
                    if (result) {
                        return true;
                    }
                    return false;
                });
            });
        }
        return true;
    }

    /**
     * Create the names needed for the SQL query. Field names are prepended with the table name. Optional JOIN statements
     * are created and joined based on the primary key of the linked ledger. The model contains all the needed
     * information to create these SQL statement parts.
     *
     * @param tableName
     * @param model
     * @returns {Promise<{fieldnames: [], joinStatement: []}>}
     */
    async prepareNames(tableName, model, fields = null) {
        let fieldNames = [];
        let joinStatement = [];
        for (const [fieldName, fieldOptions] of Object.entries(model)) {
          //Check if any of the requested fields is a nested one (DataType:LEDGER)
          if ((fields == null) || (fields.indexOf(fieldName) != -1)) {
            if ((fieldOptions.type) && (fieldOptions.type.name.toLowerCase() == 'ledger')) {
              joinStatement.push(' JOIN ' + fieldOptions.model.tableName + ' ON ' + tableName + '.' + fieldName + '=' + fieldOptions.model.tableName + '.' + fieldOptions.model.primaryKey);
              fieldNames.push(fieldOptions.model.tableName + ' AS ' + fieldName);
            } else {
              fieldNames.push(tableName + '.' + fieldName);
            }
          }
        };
        return {
            fieldNames: fieldNames,
            joinStatement: joinStatement
        };
    }

    /**
     * Creates a human readable object from the results that the QLDB returns. The results is in a 'getResultList' format.
     * If there is only one result-object only this first object is returned. Else an array of objects is returned.
     *
     * @param results
     * @returns {*}
     */
    createObjectFromResults(results, singleResultReturn = false) {
        let returnObject = [];
        if (singleResultReturn) {
            return JSON.parse(JSON.stringify(results[0]));
        }
        results.forEach(result=>{
            let record = JSON.parse(JSON.stringify(result));
            returnObject.push(record)
        });
        return returnObject;
    }

    /**
     * Unified faster way to get the active session
     *
     * @returns {Promise<QldbSession>}
     */
    async getSession() {
        if (!this.qldbDriver) throw new Error('Ledger init failed');
        this.qldbSession = await this.qldbDriver.getSession()
        return this.qldbSession;
    }

    /**
     * Close the current session
     *
     * @returns {Promise<void>}
     */
    async closeSession() {
        if (null != this.qldbSession) {
            this.qldbSession.close();
        }
    }

    /**
     * Executes the query and returns a binary result list
     *
     * @param query
     * @returns {Promise<Reader[]>}
     */
    async executeStatement(query) {
        let result;
        let response;
        //Handling session till the bug fixed for v2.0 driver. getSession method creates new session if expired before making transactions. hence invalid token exception may not occur.
        await this.getSession()
        await this.qldbDriver.executeLambda(async (txn) => {
            result = await txn.execute(query).then((result) => {
                return result;
            });
            response = result.getResultList();
        });
        return response;
    }

    /**
     * Fake ordering since PartiQL does not support it yet.
     *
     * @param object
     * @param fieldName
     * @returns {Promise<void>}
     */
    fakeOrdering(object, args) {
        const fieldNames = Object.keys(args.order);
        const fieldName = fieldNames[0];
        const direction = (args.order[fieldName] == 'asc') ? -1 : 1;
        object.sort(function(a, b) {
            let fieldA = a[fieldName].toUpperCase(); // ignore upper and lowercase
            let fieldB = b[fieldName].toUpperCase(); // ignore upper and lowercase
            if (fieldA < fieldB) {
                return direction;
            }
            if (fieldA > fieldB) {
                return direction * -1;
            }
            // names must be equal
            return 0;
        });
        return object;
    }

    /**
     * Fake pagination since PartiQL does not support it yet.
     *
     * @param object
     * @param args
     * @returns {*}
     */
    fakePagination(object, args) {
        const {offset, limit} = args;
        const results = object.length;
        let page = object.slice(offset, offset + limit);
        page.rows = results;
        return page;
    }
}

module.exports = {
    qldbConnect,
}
