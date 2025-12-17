const mongoose = require("mongoose");

const ContractSchema = new mongoose.Schema({
    user_id: {
        type: Number,
        ref: "User",
        default: null
    },
    contractName: {
        type: String,
        required: true,
    },
    description: {
        type: String,
        default: ""
    },
    startDate: {
         type: String,
        required: true
    },
    endDate: {
        type: String,
        required: true,
    },
    // artistPercentage: {
    //     type: Number,
    //     default: 0,
    //     min: 0,
    //     max: 100
    // },
    labelPercentage: {
        type: Number,
        default: 0,
        min: 0,
        max: 100
    },
    // producerPercentage: {
    //     type: Number,
    //     default: 0,
    //     min: 0,
    //     max: 100
    // },
    pdf: {
        type: String, // File path or file name
        required: true,
    },
    status: {
        type: String,
        enum: ["active", "inactive", "expired"],
        default: "active",
    },
}, {
    timestamps: true,
    versionKey: false,
});

module.exports = mongoose.model("Contract", ContractSchema);