const rbac = require("../rbac/rbac");

module.exports = (permission) => {
    return async (req, res, next) => {
        const role = req.user.role;
        const allowed = await rbac.can(role, permission);

        if (!allowed) {
            return res.status(403).json({
                success: false,
                message: "Access Denied"
            });
        }

        next();
    };
};
