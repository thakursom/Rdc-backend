const jwt = require("jsonwebtoken");
const ResponseService = require("../services/responseService");

module.exports = (req, res, next) => {
    const token = req.headers.authorization?.split(" ")[1];
    if (!token) return ResponseService.error(res, "No token provided", 401);

    jwt.verify(token, process.env.JWT_SECRET, (err, decoded) => {
        if (err && err.name === "TokenExpiredError") {
            return ResponseService.error(res, "Token expired", 401);
        }

        if (err) {
            return ResponseService.error(res, "Invalid token", 403);
        }

        req.user = decoded;
        next();
    });
};
