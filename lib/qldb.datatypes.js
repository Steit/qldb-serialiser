
class BOOLEAN {
  constructor() {
    this.name = 'boolean'
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
class JSON {
  constructor() {
    this.name = 'json'
  }
}
const DataTypes = module.exports = {
  BOOLEAN,
  STRING,
  NUMBER,
  TIMESTAMP,
  OBJECT,
  LEDGER,
  ARRAY,
  JSON
}

module.exports = {
  DataTypes
}
