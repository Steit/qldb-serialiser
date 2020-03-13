const { PooledQldbDriver } = require('amazon-qldb-driver-nodejs');
const { parseIon } = require('../helpers/ion.helper');

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
        this.tableNames = await this.getSession().then((qldbSession) => {
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
        return this.getSession().then((qldbSession) => {
            return this.buildSqlInsert(tableName, model).then((sqlBuilder) => {
                return this.executeStatement(sqlBuilder).then((result) => {
                    if (result) {
                        return this.createObjectFromResults(result);
                    }
                    return false;
                })
            });
        });
    }

    /**
     * Update an existing record with the data in the model
     *
     * @param tableName
     * @param model
     * @returns {Promise<Result>}
     */
    async update(tableName, args) {
        return this.getSession().then((qldbSession) => {
            return this.buildSqlUpdate(tableName, args).then((sqlBuilder) => {
                return this.executeStatement(sqlBuilder).then((result) => {
                    if (result) {
                        return this.createObjectFromResults(result);
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
        return this.getSession().then((qldbSession) => {
            return this.buildSqlSelect(tableName, model, args).then((sqlBuilder) => {
                return this.executeStatement(sqlBuilder).then((result) => {
                    if (result) {
                        return this.createObjectFromResults(result);
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
        return this.getSession().then((qldbSession) => {
            return this.buildSqlSelect(tableName, model, args).then((sqlBuilder) => {
                return this.executeStatement(sqlBuilder).then((result) => {
                    if (result) {
                        return this.createObjectFromResults(result);
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
        return this.getSession().then((qldbSession) => {
            return this.buildMetaSqlSelect(tableName, args).then((sqlBuilder) => {
                return this.executeStatement(sqlBuilder).then((result) => {
                    if (result) {
                        return this.createObjectFromResults(result);
                    }
                    return false;
                })
            });
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
        let sqlSelectFields = '*';
        if (args.fields) {
            sqlSelectFields = args.fields.join(', ');
        }
        const selectNames = await this.prepareNames(tableName, model);
        sqlSelectFields = selectNames.fieldnames.join(', ');

        let onStatement = '';
        if (selectNames.joinStatement.length > 0) {
            onStatement = selectNames.joinStatement.join(' ')
        }
        // Get the WHERE part for the query
        const sqlWhere = await this.createSqlWhere(args, tableName);
        return 'SELECT ' + sqlSelectFields + ' FROM ' + tableName + onStatement  + sqlWhere + ';';
    }

    /**
     * Builds the SQL query for updates. Based on the model, only first level updates are possible, no nested updates yet.
     *
     * @param tableName
     * @param args object with a .where that holds the filter conditions and a .fields array that folds the fields
     *      that should be updated. There is no check if the updated data if is different from the existing one.
     * @returns string}
     */
    async buildSqlUpdate(tableName, args) {
        // Build the UPDATE part of the query
        let sqlUpdate = '';

        for (const [key, value] of Object.entries(args.fields)) {
            sqlUpdate = sqlUpdate + tableName + "." + key + " = '"+ value +"' , ";
        }
        // Create the WHERE condition for the update
        const sqlWhere = await this.createSqlWhere(args, tableName);
        return 'UPDATE ' + tableName + ' SET ' + sqlUpdate.substr(0, sqlUpdate.length - 2)  + sqlWhere +";";
    }

    /**
     * Build the WHERE part and the ionize part for the query
     *
     * @param args
     * @param tableName
     * @returns {string}
     */
    createSqlWhere (args, tableName) {
        // Build the WHERE part and the ionize part for the query
        let sqlWhere = '';
        if (args.where) {
            for (const [key, value] of Object.entries(args.where)) {
                sqlWhere = sqlWhere + tableName + "." + key + " = '"+ value +"' AND ";
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
            return this.getSession().then((qldbSession) => {
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
     * Creates a human readable object from the results that the QLDB returns. The results is in a 'getResultList' format.
     * If there is only one result-object only this first object is returned. Else an array of objects is returned.
     *
     * @param results
     * @returns {*}
     */
    createObjectFromResults(results) {
        const returnObject = results.map(parseIon);
        if (returnObject.length == 1) {
            return returnObject[0];
        }
        return returnObject;
    }

    /**
     * Unified faster way to get the active session
     *
     * @returns {Promise<QldbSession>}
     */
    async getSession() {
        if (!this.qldbDriver) throw new Error('Ledger init failed');

        return this.qldbDriver.getSession();
    }

    /**
     * Executes the query and returns a binary result list
     *
     * @param query
     * @returns {Promise<Reader[]>}
     */
    async executeStatement(query) {
        const session = await this.getSession();
        let binaryResult = await session.executeStatement(query);
        return binaryResult.getResultList();
    }


}

module.exports = {
    qldbConnect,
}