"use strict";
/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
var amazon_qldb_driver_nodejs_1 = require("amazon-qldb-driver-nodejs");
var ion_js_1 = require("ion-js");
var LogUtil_1 = require("./LogUtil");
/**
 * Returns the string representation of a given BlockResponse.
 * @param blockResponse The BlockResponse to convert to string.
 * @returns The string representation of the supplied BlockResponse.
 */
function blockResponseToString(blockResponse) {
    var stringBuilder = "";
    if (blockResponse.Block.IonText) {
        stringBuilder = stringBuilder + "Block: " + blockResponse.Block.IonText + ", ";
    }
    if (blockResponse.Proof.IonText) {
        stringBuilder = stringBuilder + "Proof: " + blockResponse.Proof.IonText;
    }
    stringBuilder = "{" + stringBuilder + "}";
    var writer = ion_js_1.makePrettyWriter();
    var reader = ion_js_1.makeReader(stringBuilder);
    writer.writeValues(reader);
    return ion_js_1.decodeUtf8(writer.getBytes());
}
exports.blockResponseToString = blockResponseToString;
/**
 * Returns the string representation of a given GetDigestResponse.
 * @param digestResponse The GetDigestResponse to convert to string.
 * @returns The string representation of the supplied GetDigestResponse.
 */
function digestResponseToString(digestResponse) {
    var stringBuilder = "";
    if (digestResponse.Digest) {
        stringBuilder += "Digest: " + JSON.stringify(ion_js_1.toBase64(digestResponse.Digest)) + ", ";
    }
    if (digestResponse.DigestTipAddress.IonText) {
        stringBuilder += "DigestTipAddress: " + digestResponse.DigestTipAddress.IonText;
    }
    stringBuilder = "{" + stringBuilder + "}";
    var writer = ion_js_1.makePrettyWriter();
    var reader = ion_js_1.makeReader(stringBuilder);
    writer.writeValues(reader);
    return ion_js_1.decodeUtf8(writer.getBytes());
}
exports.digestResponseToString = digestResponseToString;
/**
 * Get the document IDs from the given table.
 * @param txn The {@linkcode TransactionExecutor} for lambda execute.
 * @param tableName The table name to query.
 * @param field A field to query.
 * @param value The key of the given field.
 * @returns Promise which fulfills with the document ID as a string.
 */
function getDocumentId(txn, tableName, field, value) {
    return __awaiter(this, void 0, void 0, function () {
        var query, parameter, documentId;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    query = "SELECT id FROM " + tableName + " AS t BY id WHERE t." + field + " = ?";
                    parameter = amazon_qldb_driver_nodejs_1.createQldbWriter();
                    parameter.writeString(value);
                    return [4 /*yield*/, txn.executeInline(query, [parameter]).then(function (result) {
                            var resultList = result.getResultList();
                            if (resultList.length === 0) {
                                throw new Error("Unable to retrieve document ID using " + value + ".");
                            }
                            documentId = getFieldValue(resultList[0], ["id"]);
                        })["catch"](function (err) {
                            LogUtil_1.error("Error getting documentId: " + err);
                        })];
                case 1:
                    _a.sent();
                    return [2 /*return*/, documentId];
            }
        });
    });
}
exports.getDocumentId = getDocumentId;
/**
 * Function which, given a reader and a path, traverses through the reader using the path to find the value.
 * @param ionReader The reader to operate on.
 * @param path The path to find the value.
 * @returns The value obtained after traversing the path.
 */
function getFieldValue(ionReader, path) {
    ionReader.next();
    ionReader.stepIn();
    return recursivePathLookup(ionReader, path);
}
exports.getFieldValue = getFieldValue;
/**
 * Helper method that traverses through the reader using the path to find the value.
 * @param ionReader The reader to operate on.
 * @param path The path to find the value.
 * @returns The value, or undefined if the provided path does not exist.
 */
function recursivePathLookup(ionReader, path) {
    if (path.length === 0) {
        // If the path's length is 0, the current ionReader node is the value which should be returned.
        if (ionReader.type() === ion_js_1.IonTypes.LIST) {
            var list = [];
            ionReader.stepIn(); // Step into the list.
            while (ionReader.next() != null) {
                var itemInList = recursivePathLookup(ionReader, []);
                list.push(itemInList);
            }
            return list;
        }
        else if (ionReader.type() === ion_js_1.IonTypes.STRUCT) {
            var structToReturn = {};
            var type = void 0;
            var currentDepth = ionReader.depth();
            ionReader.stepIn();
            while (ionReader.depth() > currentDepth) {
                // In order to get all values within the struct, we need to visit every node.
                type = ionReader.next();
                if (type === null) {
                    // End of the container indicates that we need to step out.
                    ionReader.stepOut();
                }
                else {
                    structToReturn[ionReader.fieldName()] = recursivePathLookup(ionReader, []);
                }
            }
            return structToReturn;
        }
        return ionReader.value();
    }
    else if (path.length === 1) {
        // If the path's length is 1, the single value in the path list is the field should to be returned.
        while (ionReader.next() != null) {
            if (ionReader.fieldName() === path[0]) {
                path.shift(); // Remove the path node which we just entered.
                return recursivePathLookup(ionReader, path);
            }
        }
    }
    else {
        // If the path's length >= 2, the Ion tree needs to be traversed more to find the value we're looking for.
        while (ionReader.next() != null) {
            if (ionReader.fieldName() === path[0]) {
                ionReader.stepIn(); // Step into the IonStruct.
                path.shift(); // Remove the path node which we just entered.
                return recursivePathLookup(ionReader, path);
            }
        }
    }
    // If the path doesn't exist, return undefined.
    return undefined;
}
exports.recursivePathLookup = recursivePathLookup;
/**
 * Sleep for the specified amount of time.
 * @param ms The amount of time to sleep in milliseconds.
 * @returns Promise which fulfills with void.
 */
function sleep(ms) {
    return new Promise(function (resolve) { return setTimeout(resolve, ms); });
}
exports.sleep = sleep;
/**
 * Returns the string representation of a given ValueHolder.
 * @param valueHolder The ValueHolder to convert to string.
 * @returns The string representation of the supplied ValueHolder.
 */
function valueHolderToString(valueHolder) {
    var stringBuilder = "{ IonText: " + valueHolder.IonText + "}";
    var writer = ion_js_1.makePrettyWriter();
    var reader = ion_js_1.makeReader(stringBuilder);
    writer.writeValues(reader);
    return ion_js_1.decodeUtf8(writer.getBytes());
}
exports.valueHolderToString = valueHolderToString;
/**
 * Converts a given value to Ion using the provided writer.
 * @param value The value to covert to Ion.
 * @param ionWriter The Writer to pass the value into.
 * @throws Error: If the given value cannot be converted to Ion.
 */
function writeValueAsIon(value, ionWriter) {
    switch (typeof value) {
        case "string":
            ionWriter.writeString(value);
            break;
        case "boolean":
            ionWriter.writeBoolean(value);
            break;
        case "number":
            ionWriter.writeInt(value);
            break;
        case "object":
            if (Array.isArray(value)) {
                // Object is an array.
                ionWriter.stepIn(ion_js_1.IonTypes.LIST);
                for (var _i = 0, value_1 = value; _i < value_1.length; _i++) {
                    var element = value_1[_i];
                    writeValueAsIon(element, ionWriter);
                }
                ionWriter.stepOut();
            }
            else if (value instanceof Date) {
                // Object is a Date.
                ionWriter.writeTimestamp(ion_js_1.Timestamp.parse(value.toISOString()));
            }
            else if (value instanceof ion_js_1.Decimal) {
                // Object is a Decimal.
                ionWriter.writeDecimal(value);
            }
            else if (value === null) {
                ionWriter.writeNull(ion_js_1.IonTypes.NULL);
            }
            else {
                // Object is a struct.
                ionWriter.stepIn(ion_js_1.IonTypes.STRUCT);
                for (var _a = 0, _b = Object.keys(value); _a < _b.length; _a++) {
                    var key = _b[_a];
                    ionWriter.writeFieldName(key);
                    writeValueAsIon(value[key], ionWriter);
                }
                ionWriter.stepOut();
            }
            break;
        default:
            throw new Error("Cannot convert to Ion for type: " + (typeof value) + ".");
    }
}
exports.writeValueAsIon = writeValueAsIon;
