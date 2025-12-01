// models/logModel.js
const mongoose = require("mongoose");

const logSchema = new mongoose.Schema({
    user_id: {
        type: Number,
        ref: "User",
        default: null
    },
    email: {
        type: String,
        default: null
    },
    action: {
        type: String,
        required: true
    },
    description: {
        type: String,
        required: true
    },
    oldData: {
        type: Object,
        default: {}
    },
    newData: {
        type: Object,
        default: {}
    },
    ip: {
        type: String
    },
    userAgent: {
        type: String
    },
},
    {
        timestamps: true,
        versionKey: false
    }

);

module.exports = mongoose.model("Log", logSchema);
