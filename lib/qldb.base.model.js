const cloneDeep = require('lodash/cloneDeep');

const {DataTypes} = require('./qldb.datatypes');

class Ledger {
    constructor(qldbConnection, tableName, model, options) {
        this.qldbConnection = qldbConnection;
        this.tableName = tableName;
        this.primaryKey = null;
        this.model = model;

        this.options = options;
        if (this.options.timestamps == true) {
            let date = new Date().toISOString();
            this.model['createdAt'] = {
                type: DataTypes.TIMESTAMP,
                value: date
            };
            this.model['updatedAt'] = {
                type: DataTypes.TIMESTAMP,
                value: date
            };
        }

        // QLDB values
        this.metadata = null;
        this.blockAddress = null;
        this.hash = null;

        // Bindings
        this.mapDataToModel = this.mapDataToModel.bind(this);
        this.getByPk = this.getByPk.bind(this);
        this.getByDocumentId = this.getByDocumentId.bind(this);

        // instance methods
        this.instanceMethods = {};
        if (this.options.instanceMethods) {
            this.instanceMethods = this.options.instanceMethods;
        }

        this.getPrimaryKeyName();
    }

    /**
     * bind instance method to returning object which defined in model class
     *
     * @param {object} modelObject
     * @return {object}
     */
    bindInstanceMethod(modelObject) {
        Object.entries(this.instanceMethods).forEach(([k, v]) => {
            Object.defineProperty(modelObject, k, {
                value: v.bind(modelObject),
            });
        });

        return modelObject;
    }

    /**
     * build model class instance based on queried object
     *
     * @param {*} raw - queried object
     * @return {object}
     */
    buildModelObject(raw) {
        if (typeof raw !== 'object') {
            return raw;
        }

        let result = raw;

        return this.bindInstanceMethod(result);
    }

    /**
     * Find all records by the supplied arguments
     *
     * @param args
     * @returns {Promise<*>}
     */
    async getBy(args)  {
        const results = await this.qldbConnection.findBy(this.tableName, this.model, args);

        let boundResults = results.map((res) => this.buildModelObject(res));
        // Add the pagination info to to the bound results
        if (results.rows) {
            boundResults.rows = results.rows;
        }
        return boundResults;
    };

    /**
     * Find one records by the supplied arguments, if an array is found only the first object is returned
     *
     * @param args
     * @returns {Promise<*>}
     */
    async getOneBy(args)  {
        const result = await this.qldbConnection.findOneBy(this.tableName, this.model, args);

        return this.buildModelObject(result);
    };

    /**
     * Delete one or more records. The option {recursive: true} in the arguments will delete linked entries that are
     * defined in the model as LEDGER. Note that the record(s) only are removed from the active table. They will remain
     * in the _ql_committed tables.
     *
     * @param args
     * @returns {Promise<void>}
     */
    async delete(args) {
        return await this.qldbConnection.delete(this.tableName, this.model, args);
    }

    /**
     * Find a record by its defined Primary Key. The model needs to have one field as 'primaryKey: true,'
     *
     * @param id
     * @returns {Promise<*>}
     */
    async getByPk(id)  {
        let args = {};
        const primaryKeyName = await this.getPrimaryKeyName();
        if (primaryKeyName) {
            args[primaryKeyName] = id;
        } else {
            return false;
        }
        const result = await this.qldbConnection.findOneBy(this.tableName, this.model,{where:args});

        return this.buildModelObject(result);
    };

    /**
     * Find historic changes of a record based on the primary key. The model needs to have one field as 'primaryKey: true,'
     * If the start and/or end date lies in the future there is an error returned.
     *
     * @param PkId
     * @param startDate
     * @param endDate
     * @returns {Promise<boolean|*>}
     */
    async getHistoryByPk(PkId, startDate = null, endDate = null)  {
        const primaryKeyName = await this.getPrimaryKeyName();
        if (!primaryKeyName) {
            return false;
        }
        const now = new Date();
        if ((Date.parse(startDate) > now) || (Date.parse(endDate) > now)) {
            return 'invalid_dates';
        }
        let whereArgs = {}
        whereArgs[primaryKeyName] = PkId;
        const args = {
            where: whereArgs,
            useMetaData: false,
            startDate: startDate,
            endDate: endDate
        }

        const results = await this.qldbConnection.getHistory(this.tableName, args);

        return results.map((res) => {
            const { data } = res;
            if (data) {
                Object.assign(res, { data: this.buildModelObject(data) });
            }

            return res;
        });
    };

    /**
     * Find historic changes of a record based on the document id. If the start and/or end date lies in the future there
     * is an error returned.
     *
     * @param documentId
     * @param startDate
     * @param endDate
     * @returns {Promise<boolean|*>}
     */
    async getHistoryByDocumentId(documentId, startDate = null, endDate = null)  {
        const now = new Date();
        if ((Date.parse(startDate) > now) || (Date.parse(endDate) > now)) {
            return 'invalid_dates';
        }
        const args = {
            where: {id: documentId},
            useMetaData: true,
            startDate: startDate,
            endDate: endDate
        }

        const results = await this.qldbConnection.getHistory(this.tableName, args);

        return results.map((res) => {
            const { data } = res;
            if (data) {
                Object.assign(res, { data: this.buildModelObject(data) });
            }

            return res;
        });
    };

    /**
     *Find historic changes of a record based on the supplied arguments. if empty object is passed as argument, it returns history of whole table
     * @param args
     * @returns {Promise<void>}
     */
    async getHistoryBy(args) {

        const results = await this.qldbConnection.getHistory(this.tableName, args);

        return results.map((res) => {
            const { data } = res;
            if (data) {
                Object.assign(res, { data: this.buildModelObject(data) });
            }

            return res;
        });
    };

    /**
     * Gets a document by the document.id in the metadata.
     *
     * @param value
     * @returns {Promise<*>}
     */
    async getByDocumentId(value)  {
        let args = {}
        args['metadata.id'] = value;
        const results = await this.qldbConnection.findCommittedData(this.tableName,{where:args});

        return results.map((res) => {
            const { data } = res;
            if (data) {
                Object.assign(res, { data: this.buildModelObject(data) });
            }

            return res;
        });
    };

    async getDocumentIdByPK(pkValue)  {
        let args = {}
        args['data.' + this.primaryKey] = pkValue;
        const results = await this.qldbConnection.findCommittedData(this.tableName,{where:args});

        return results.map((res) => {
            const { data } = res;
            if (data) {
                Object.assign(res, { data: this.buildModelObject(data) });
            }

            return res;
        });
    };

    /**
     * Add the data to the QLDB but first map it to the model and return potential errors
     *
     * @param data
     * @returns {Promise<unknown>}
     */
    async add(data, maxDepth=3)  {
        const modelCopy = cloneDeep(this.model);
        //check if table exists or not. if not existed, creates table and insert data into it
        await this.qldbConnection.checkTableExistence(this.tableName);
        const mappedModel = await this.mapDataToModel(data, modelCopy, 0, maxDepth);
        if (mappedModel.errors) {
            return {errors: mappedModel.errors};
        }
        // return mappedModel;
        return this.qldbConnection.create(this.tableName,mappedModel).then((results) =>{
            return results;
        });
    };

    /**
     * Update a record by supplying arguments. the structure of the args object is as follows
     * args = {
     *      fields: {fieldName: FieldValue},
     *      where: {fieldName: FieldValue}
     * }
     *
     * @param args object
     * @returns {Promise<any|*[]>}
     */
    async update(args, maxDepth=3) {
        const modelCopy = cloneDeep(this.model);
        const mappedModel = await this.mapDataToModel(args.fields, modelCopy, 0, maxDepth, true);
        if (mappedModel.errors) {
            return {errors: mappedModel.errors};
        }
        if (this.options.timestamps == true){
            args.fields.updatedAt = mappedModel.updatedAt.value;
        }
        return this.qldbConnection.update(this.tableName, args, mappedModel).then((results) =>{
            return results;
        });
    }

    /**
     * get the name of the primary key of this model
     *
     * @returns {string|boolean}
     */
    async getPrimaryKeyName() {
        if (this.primaryKey !== null) {
            return this.primaryKey;
        }
        for (const [fieldName, FieldOptions] of Object.entries(this.model)) {
            if (FieldOptions.primaryKey == true) {
                this.primaryKey = fieldName;
                return fieldName;
            }
        }
        return false;
    }

    /**
     * Maps the given data to the model and validates it against the model type. Use the maxDepth to limit the amount of
     * data in the model.
     *
     * @param data          The data received at the endpoint
     * @param model         The model associated with this level
     * @param currentDepth  Active depth, used to calculate the maxdepth
     * @param maxDepth      Number of levels down that are used to prevent deadlocks and massive data usage. Defaults to 3
     * @param isUpdate      Marks the model as an update for the creation of an update model. It ignores the pk and missing checks
     *
     * @returns {Promise<{errors: []}|boolean|*>}
     */
    async mapDataToModel(data, model, currentDepth = 0, maxDepth = 3, isUpdate = false) {
        if (currentDepth >= maxDepth) {
            return true;
        }
        let errors = [];
        for (const [fieldName, fieldOptions] of Object.entries(model)) {
            // Check if the field is present in the data
            if (data[fieldName] != null) {
                let fieldType = fieldOptions.type.name.toLowerCase()
                if (fieldType == 'ledger') { // Check the values in the linked model
                    /**
                     * If the data is not an object, check if there is a primary key with the value of the data.
                     */
                    if (typeof data[fieldName] != 'object') {
                        const result = await fieldOptions.model.getByPk(data[fieldName]);
                        if (Object.keys(result).length == 0) {
                            errors.push({
                                field: fieldName,
                                message: 'document_reference_not_found',
                                value: data[fieldName]
                            });
                        }
                        // Change Ledger model to String and set value to prevent creation of a new record
                        model[fieldName].value = data[fieldName]; //result[0].metadata.id;
                        model[fieldName].type = DataTypes.STRING;
                        delete model[fieldName]['model'];
                    } else {
                        let result = await fieldOptions.model.mapDataToModel(data[fieldName], fieldOptions.model.model, currentDepth + 1, maxDepth, isUpdate);
                        if (result.errors) {
                            result.errors.forEach(error => {
                                errors.push({ field: fieldName + '.' + error.field, message: error.message });
                            });
                        }
                    }
                    model[fieldName].value = data[fieldName];
                } else if (fieldType == 'json') { // JSON data is ignored in checking and used 'as is'
                    model[fieldName].value = data[fieldName];
                } else if (typeof data[fieldName] == fieldType) { // Check if the value is of the correct type
                    if (fieldType == 'object') {
                        let result = await this.mapDataToModel(data[fieldName], fieldOptions.model, currentDepth + 1, maxDepth, isUpdate);
                        if (result.errors) {
                            result.errors.forEach(error => {
                                errors.push({ field: fieldName + '.' + error.field, message: error.message });
                            });
                        }
                    }
                    // Check if the entered field is a primary key and if so check id that already exists
                    if ((fieldOptions.primaryKey) && (!isUpdate)) {
                        const result = await this.getByPk(data[fieldName]);
                        if (Object.keys(result).length != 0) {
                            errors.push({
                                field: fieldName,
                                message: 'pk_reference_duplicate',
                                value: data[fieldName]
                            });
                        } else {
                            model[fieldName].value = data[fieldName];
                        }
                    } else {
                        model[fieldName].value = data[fieldName];
                    }
                 /**
                  * Arrays are mistaken for objects by the node typeof function. An array is an array of objects or any
                  * other type. When checking an array we need to check for one or more value sets. The code will fill
                  * the model section with the values of the last value-set. When creating the SQL for insertion these
                  * values will be ignored and the values are taken from the value member on the model level (one up).
                  */
                } else if (Array.isArray(data[fieldName])) {
                    for (const element of  data[fieldName]) {
                        let result = null;
                        // if the model is not defined in array datatype, it will map the data as is. useful for array of strings, numbers, JSON e.t.c
                        if (typeof fieldOptions.model === 'undefined') {
                            result = await this.mapDataToModel(element, fieldOptions, currentDepth, maxDepth, isUpdate);
                        }
                        // Nested arrays of linked Ledger models
                        else if ((fieldOptions.model.constructor) && (fieldOptions.model.constructor.name.toLowerCase() == 'ledger')) {
                            result = await fieldOptions.model.mapDataToModel(element, fieldOptions.model.model, currentDepth + 1, maxDepth, isUpdate);
                        } else {
                            result = await this.mapDataToModel(element, fieldOptions.model, currentDepth , maxDepth, isUpdate);
                        }
                        if (result.errors) {
                            result.errors.forEach(error => {
                                errors.push({field: fieldName + '.' + error.field , message: error.message});
                            });
                        }
                    }
                    model[fieldName].value = data[fieldName];
                } else {
                    errors.push({ field: fieldName, message: 'invalid_value', expected: fieldType, received: typeof data[fieldName]});
                }
            // If the value is not present but has a default value in the model then use that
            } else if (model[fieldName].default !== undefined) {
                model[fieldName].value = model[fieldName].default;
            // Set the value to NULL if allowed
            } else if (model[fieldName].allowNull == true) {
                model[fieldName].value = null;
            } else if (model[fieldName].index == true) {
                // If the field has been choosen as index
                await this.qldbConnection.checkIfIndexExists(this.tableName, model[fieldName])
            } else if ((model[fieldName].allowNull == false) && (!isUpdate)) {
                // If the value is not allowed to be null add an error
                errors.push({ field: fieldName, message: 'missing'});
            }
        };
        // If errors occurred, return them otherwise return the model.
        if (errors.length > 0) {
            return { errors: errors };
        }
        return model;
    }

}

module.exports = {
    Ledger
}
