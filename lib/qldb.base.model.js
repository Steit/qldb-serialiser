const {qldbConnection} = require('../config/qldb.connect');

class Ledger {
    constructor(tableName, model, options) {
        this.tableName = tableName;
        this.model = model;
        this.options = options;
        if (this.options.timestamps == true) {
            let date = new Date();
            this.model['createdAt'] = {value: date};
            this.model['updatedAt'] = {value: date};
        }
    }

    async getAll(args= {})  {
        return await qldbConnection.findBy(this.tableName,{where:args});
    };

    async getBy(args)  {
        return await qldbConnection.findBy(this.tableName,args);
    };

    async getByPk(value)  {
        let args = {};
        for (const [fieldName, FieldOptions] of Object.entries(this.model)) {
            if (FieldOptions.primaryKey == true) {
                args[fieldName] = value;
            }
        }
        return await qldbConnection.findOneBy(this.tableName,{where:args});
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
            return qldbConnection.create(this.tableName,this.model).then((results) =>{
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
                //Check if the value is of the correct type
                if (typeof data[fieldName] == FieldOptions.type.name.toLowerCase()) {
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

const DataTypes = module.exports = {
    BOOL,
    STRING,
    NUMBER,
    TIMESTAMP,
    OBJECT
}

module.exports = {
    Ledger,
    DataTypes
}