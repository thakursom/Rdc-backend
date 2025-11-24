const mongoose = require("mongoose");

const AppleRevenueSchema = new mongoose.Schema(
    {
        uploadId: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "RevenueUpload",
            required: true
        },
        Month: {
            type: String
        },
        Country: {
            type: String
        },
        "Apple Identifier": {
            type: String
        },
        "Membership Type": {
            type: String
        },
        Quantity: {
            type: Number
        },
        ISRC: {
            type: String
        },
        "Item Title": {
            type: String
        },
        "Item Artist": {
            type: String
        },
        "Vendor Identifier": {
            type: String
        },
        "Label Name": {
            type: String
        },
        "Label Code": {
            type: String
        },
        "Total Revenue": {
            type: Number
        },
        "Sub Label Code": {
            type: String
        }
    },
    {
        timestamps: true,
        versionKey: false
    }
);

module.exports = mongoose.model("AppleRevenue", AppleRevenueSchema);
