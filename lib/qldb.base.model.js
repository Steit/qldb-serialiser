class Ledger {
    constructor(qldbConnection, tableName, model, options) {
        this.qldbConnection = qldbConnection;
        this.tableName = tableName;
        this.model = model;
        this.options = options;
        if (this.options.timestamps == true) {
            let date = new Date();
            this.model['createdAt'] = {value: date};
            this.model['updatedAt'] = {value: date};
        }
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
     * Add the data to the QLDB but first map it to the model and return potential errors
     *
     * @param data
     * @returns {Promise<unknown>}
     */
    async add(data)  {
        return this.mapDataToModel(data).then((errors) => {
            if (errors) {
                return errors;
            }
            return this.qldbConnection.create(this.tableName,this.model).then((results) =>{
                return results;
            });
        });
    };

    /**
     * Maps the given data to the model and validates it against the model type
     *
     * @param data
     * @returns {Promise<boolean|[]>}
     */
    async mapDataToModel(data) {
        let errors = [];
        for (const [fieldName, FieldOptions] of Object.entries(this.model)) {
            // Check if the field is present in the data
            if (data[fieldName]) {
                if (FieldOptions.type.name.toLowerCase() == 'ledger') { //Check if the value is another, linked model
                    let result = await FieldOptions.model.add(data[fieldName]);
                    if (Object.keys(result).indexOf('documentId') < 0) {
                        errors.push({field: fieldName, message: result});
                    }
                    this.model[fieldName].value = result.documentId;
                } else if (typeof data[fieldName] == FieldOptions.type.name.toLowerCase()) { //Check if the value is of the correct type
                    this.model[fieldName].value = data[fieldName];
                } else {
                    errors.push({ field: fieldName, message: 'invalid_value'});
                }
            // If the value is not present but has a default value in the model then use that
            } else if (this.model[fieldName].default !== undefined) {
                this.model[fieldName].value = this.model[fieldName].default;
            // Set the value to NULL if allowed
            } else if (this.model[fieldName].allowNull == true) {
                this.model[fieldName].value = null;
            // If the value is not allowed to be null add an error
            } else if (this.model[fieldName].allowNull == false) {
                errors.push({ field: fieldName, message: 'missing'});
            }
        };
        // If errors occurred, return them otherwise return true.
        if (errors.length > 0) {
            return errors;
        }
        return false;
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
const DataTypes = module.exports = {
    BOOL,
    STRING,
    NUMBER,
    TIMESTAMP,
    OBJECT,
    LEDGER
}

module.exports = {
    Ledger,
    DataTypes
}