'use strict';

/**
  * The entry point.
  *
  * @module Sequelize
  */
const {qldbConnect} = require('./lib/qldb.connect');

const {Ledger, DataTypes} = require('./lib/qldb.base.model');

module.exports = {
    qldbConnect,
    Ledger,
    DataTypes
}
