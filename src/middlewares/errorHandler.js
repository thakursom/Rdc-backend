const ResponseService = require("../services/responseService");

const errorHandler = (err, req, res, next) => {
    console.error("Error:", err.stack);

    const statusCode = err.statusCode || 500;
    const message = err.message || "Internal Server Error";

    return ResponseService.error(res, message, statusCode, err);
};

module.exports = errorHandler;
