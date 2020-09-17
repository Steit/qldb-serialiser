const arrayStringify = (s) => {
    return JSON.stringify(s)
        .replace(/"/ig, "'")
        .replace(/\\n/ig, "\n")
        .replace(/\\b/ig, "\b")
        .replace(/\\t/ig, "\t")
        .replace(/\\f/ig, "\f")
        .replace(/\\r/ig, "\r");
};

module.exports = {
    arrayStringify,
}