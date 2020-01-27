class Ledger {
    constructor(qldbConnection, tableName, model, options) {
        this.qldbConnection = qldbConnection;
        this.tableName = tableName;
        this.model = model;
        this.options = options;
        if (this.options.timestamps == true) {
            let date = new Date();
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
        this.hash = null

        // Bindings
        this.mapDataToModel = this.mapDataToModel.bind(this);
        this.getByPk = this.getByPk.bind(this);
        this.getByDocumentId = this.getByDocumentId.bind(this);
    }

    /**
     * Retreive all records in the table, NOTE: Pagination is not yet supported, long result lists might be loaded.
     *
     * @param args
     * @returns {Promise<*>}
     */
    async getAll(args= {})  {
        return await this.qldbConnection.findBy(this.tableName, this.model, {where:args});
    };

    /**
     * Find records by the supplied arguments
     *
     * @param args
     * @returns {Promise<*>}
     */
    async getBy(args)  {
        return await this.qldbConnection.findBy(this.tableName, this.model, args);
    };

    /**
     * Find a record by its defined Primary Key. The model needs to have one field as 'primaryKey: true,'
     *
     * @param value
     * @returns {Promise<*>}
     */
    async getByPk(value)  {
        let args = {};
        for (const [fieldName, FieldOptions] of Object.entries(this.model)) {
            if (FieldOptions.primaryKey == true) {
                args[fieldName] = value;
            }
        }
        return await this.qldbConnection.findOneBy(this.tableName, this.model,{where:args});
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
        return await this.qldbConnection.findCommittedData(this.tableName,{where:args});
    };


    /**
     * Add the data to the QLDB but first map it to the model and return potential errors
     *
     * @param data
     * @returns {Promise<unknown>}
     */
    async add(data)  {
        const mappedModel = await this.mapDataToModel(data, this.model);
        if (mappedModel.errors) {
            return mappedModel.errors;
        }
        // return mappedModel;
        return this.qldbConnection.create(this.tableName,mappedModel).then((results) =>{
            return results;
        });
    };

    /**
     * Maps the given data to the model and validates it against the model type. Use the maxDepth to limit the amount of
     * data in the model.
     *
     * @param data          The data received at the endpoint
     * @param model         The model associated with this level
     * @param currentDepth  Active depth, used to calculate the maxdepth
     * @param maxDepth      Number of levels down that are used to prevent deadlocks and massive data usage. Defaults to 3
     *
     * @returns {Promise<{errors: []}|boolean|*>}
     */
    async mapDataToModel(data, model, currentDepth = 0, maxDepth = 3) {
        if (currentDepth >= maxDepth) {
            return true;
        }
        let errors = [];
        for (const [fieldName, fieldOptions] of Object.entries(model)) {
            // Check if the field is present in the data
            if (data[fieldName]) {
                let fieldType = fieldOptions.type.name.toLowerCase()
                if (fieldType == 'ledger') { // Check the values in the linked model
                    /**
                     * If the data is not an object, check if there is a primary key with the value of the data.
                     */
                    if (typeof data[fieldName] != 'object') {
                        const result = await fieldOptions.model.getByDocumentId(data[fieldName]);
                        if (Object.keys(result).length == 0) {
                            errors.push({field: fieldName, message: 'pk_reference_not_found', value: data[fieldName]});
                        }
                        model[fieldName].value = result.metadata.id;
                    } else {
                        let result = await fieldOptions.model.mapDataToModel(data[fieldName], fieldOptions.model.model, currentDepth + 1, maxDepth);
                        if (result.errors) {
                            errors.push({field: fieldName, message: result});
                        }
                    }
                    model[fieldName].value = data[fieldName];
                } else if (typeof data[fieldName] == fieldType) { // Check if the value is of the correct type
                    if (fieldType == 'object') {
                        let result = await this.mapDataToModel(data[fieldName], fieldOptions.model, currentDepth + 1, maxDepth);
                        if (result.errors) {
                            errors.push({field: fieldName, message: result});
                        }
                    }
                    // Check if the entered field is a primary key and if so check id that already exists
                    if (fieldOptions.primaryKey) {
                        const result = await this.getByPk(data[fieldName]);
                        if (Object.keys(result).length != 0) {
                            errors.push({field: fieldName, message: 'pk_reference_duplicate', value: data[fieldName]});
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
                        let result = await this.mapDataToModel(element, fieldOptions.model, currentDepth , maxDepth);
                        if (result.errors) {
                            errors.push({field: fieldName, message: result});
                        }
                        result = null;
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
            // If the value is not allowed to be null add an error
            } else if (model[fieldName].allowNull == false) {
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

class BOOL {
    constructor() {
        this.name = 'bool'
    }
}

class STRING {
    constructor() {
        this.name = 'string'
    }
}
class NUMBER {
    constructor() {
        this.name = 'number'
    }
}
class INTEGER {
    constructor() {
        this.name = 'int'
    }
}
class TIMESTAMP {
    constructor() {
        this.name = 'timestamp'
    }
}
class OBJECT {
    constructor() {
        this.name = 'object'
    }
}
class LEDGER {
    constructor() {
        this.name = 'ledger'
    }
}
class ARRAY {
    constructor() {
        this.name = 'array'
    }
}
const DataTypes = module.exports = {
    BOOL,
    STRING,
    NUMBER,
    TIMESTAMP,
    OBJECT,
    LEDGER,
    ARRAY
}

module.exports = {
    Ledger,
    DataTypes
}