const mongoose = require("mongoose");

const AmazonRevenueSchema = new mongoose.Schema(
    {
        uploadId: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "RevenueUpload",
            required: true
        },
        Month: {
            type: String
        },
        "territory code": {
            type: String
        },
        "Subscription Plan": {
            type: String
        },
        "Track Asin": {
            type: String
        },
        ISRC: {
            type: String
        },
        "Proprietary Track Id": {
            type: String
        },
        "Track Name": {
            type: String
        },
        "Proprietary Album Id": {
            type: String
        },
        "Digital Album Upc": {
            type: String
        },
        "Album Name": {
            type: String
        },
        "Artist Name": {
            type: String
        },
        "Label Name": {
            type: String
        },
        "Label ID": {
            type: String
        },
        streams: {
            type: Number
        },
        "Offline Plays": {
            type: Number
        },
        "Total Plays": {
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

module.exports = mongoose.model("AmazonRevenue", AmazonRevenueSchema);
