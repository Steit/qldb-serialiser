const  _ionJs = require("ion-js");
 /**
  * Parses the ion document(s) created by the QLDB. The IonTypes.STRUCT and IonTypes.LIST types have a recurrent call
  * to this function.
  *
  * @param ion
  * @returns {{}|null|[]}
  */
 function parseIon(ion) {
    const structToReturn = {};

    if (ion.type() === null) {
        ion.next();
    }

    let fieldValue = null;
    switch (ion.type()) {
        case _ionJs.IonTypes.STRING:
            fieldValue = ion.stringValue();
            break;
        case _ionJs.IonTypes.BOOL:
            fieldValue = ion.booleanValue();
            break;
        case _ionJs.IonTypes.INT:
            fieldValue = ion.numberValue();
            break;
        case _ionJs.IonTypes.TIMESTAMP:
            fieldValue = ion.timestampValue().toString();
            break;
        case _ionJs.IonTypes.BLOB:
            fieldValue = ion.blobValue;
            break;
        case _ionJs.IonTypes.STRUCT:
            let type;
            const currentDepth = ion.depth();
            let structToReturn = {};
            ion.stepIn();

            while (ion.depth() > currentDepth) {
                type = ion.next();
                if (type === null) {
                    ion.stepOut();
                } else {
                    structToReturn[ion.fieldName()] = parseIon(ion);
                }
            }
            return structToReturn;
            break;
        case _ionJs.IonTypes.LIST:
            const list = [];
            ion.stepIn();

            while (ion.next() != null) {
                const itemInList = parseIon(ion);
                list.push(itemInList);
            }
            return list;
            break;
        default:
            fieldValue = ion.value;
    }

     return fieldValue;
}

module.exports = {
    parseIon
}
