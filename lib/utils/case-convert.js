/** @module utils */
const { camelCase, snakeCase } = require('change-case');

function _ObjKeyConv(obj, _func) {
    if (typeof _func !== 'function') {
        throw new Error('\`_func\` must be a function');
    }

    if (!obj || typeof obj !== 'object') {
        return obj;
    }

    if (Array.isArray(obj)) {
        const res = obj.map((value) => {
            return _ObjKeyConv(value, _func);
        });

        return res;
    }

    const res = Object.entries(obj)
        .reduce((acc, [key, value]) => ({
            ...acc, [_func(key)]: _ObjKeyConv(value, _func),
        }), {});

    return res;
}

/**
 * @func toCamelCase
 * @desc convert object-key in object/array object to camel case
 * @param {Object} obj - object to convert
 */
function toCamelCase(obj) {
    return _ObjKeyConv(obj, camelCase);
}

/**
 * @func toSnakeCase
 * @desc convert object-key in object/array object to snake case
 * @param {Object} obj - object to convert
 */
function toSnakeCase(obj) {
    return _ObjKeyConv(obj, snakeCase);
}

module.exports = {
    toCamelCase,
    toSnakeCase,
};
