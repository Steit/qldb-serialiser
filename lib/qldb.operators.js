const EQ = {
    name: 'eq',
    operator: ' = ',
}

const NE= {
    name: 'ne',
    operator: ' != ',
}

const IN= {
    name: 'in',
    operator: ' IN ',
}

const NOTIN= {
    name: 'notIn',
    operator: ' NOT IN ',
}

const GT= {
    name: 'gt',
    operator: ' > ',
}

const LT= {
    name: 'lt',
    operator: ' < ',
}

const GTE= {
    name: 'gte',
    operator: ' >= ',
}

const LTE= {
    name: 'lte',
    operator: ' <= ',
}

const Operators = module.exports= {
  EQ,
  NE,
  IN,
  NOTIN,
  GT,
  GTE,
  LT,
  LTE,
  getOperatorNames () {
    return ['EQ','NE','IN','NOTIN','GT','GTE','LT','LTE']
  }
}

module.exports= {
  Operators
}
