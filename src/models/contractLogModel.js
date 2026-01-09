const mongoose = require("mongoose");

const ContractLogSchema = new mongoose.Schema({
    user_id: {
        type: Number,
        ref: "User",
        default: null
    },
    contract_id: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Contract",
        required: false,
    },
    action: {
        type: String,
        enum: ["add", "update", "delete", "auto_renew"],
        required: true,
    },
    data: {
        type: Object,
        required: true,
    },
    message: {
        type: String,
        default: "",
    },
    ipAddress: {
        type: String,
        default: null,
    },
},
    {
        timestamps: true,
        versionKey: false
    }
);

module.exports = mongoose.model("ContractLog", ContractLogSchema);
