const RBAC = require("easy-rbac");

const rbac = new RBAC({
    "Super Admin": {
        can: ["user:create", "user:update", "user:delete", "user:view"]
    },
    "Admin": {
        can: ["user:create", "user:update", "user:view"]
    },
    "Manager": {
        can: ["user:update", "user:view"]
    },
    "Label": {
        can: ["user:view"]
    },
    "Sub Label": {
        can: ["user:view"]
    }
});

module.exports = rbac;
