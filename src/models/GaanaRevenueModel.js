const mongoose = require("mongoose");

const GaanaRevenueSchema = new mongoose.Schema(
    {
        uploadId: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "RevenueUpload",
            required: true
        },
        Artist: {
            type: String
        },
        "Film / Non-Film": {
            type: String
        },
        Language: {
            type: String
        },
        ISRC: {
            type: String
        },
        "Label Name": {
            type: String
        },
        "Label Code": {
            type: String
        },
        "Free Playouts": {
            type: Number
        },
        "Paid playouts": {
            type: Number
        },
        "Total Playouts": {
            type: Number
        },
        "Total": {
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

module.exports = mongoose.model("GaanaRevenue", GaanaRevenueSchema);
