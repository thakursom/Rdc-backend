const mongoose = require('mongoose');

const rolePermissionSchema = new mongoose.Schema({
    role: {
        type: String,
        required: true,
        unique: true,
        enum: ['Super Admin', 'Manager', 'Label']
    },
    allowedKeys: {
        type: [String],
        default: []
    },
}, {
    timestamps: true,
    versionKey: false
});

module.exports = mongoose.model('RolePermission', rolePermissionSchema);