const mongoose = require("mongoose");

const RoleSchema = new mongoose.Schema({
    role: {
        type: String,
        required: true
    },
    permissions: {
        type: [String],
        default: []
    }
}, {
    timestamps: true,
    versionKey: false
});

module.exports = mongoose.model("Role", RoleSchema);
