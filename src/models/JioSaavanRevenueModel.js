const mongoose = require("mongoose");

const JioSaavanRevenueSchema = new mongoose.Schema(
    {
        uploadId: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "RevenueUpload",
            required: true
        },
        Months: {
            type: String
        },
        Country: {
            type: String
        },
        "Track Name": {
            type: String
        },
        "Album Name": {
            type: String
        },
        "Artist Name": {
            type: String
        },
        ISRC: {
            type: String
        },
        UPC: {
            type: String
        },
        Language: {
            type: String
        },
        "Label Name": {
            type: String
        },
        "Label ID": {
            type: String
        },
        "Total Streams": {
            type: Number
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

module.exports = mongoose.model("JioSaavanRevenue", JioSaavanRevenueSchema);
