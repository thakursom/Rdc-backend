const mongoose = require("mongoose");

const FacebookRevenueSchema = new mongoose.Schema(
    {
        uploadId: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "RevenueUpload",
            required: true
        },
        Month: {
            type: String
        },
        service: {
            type: String
        },
        country: {
            type: String
        },
        product: {
            type: String
        },
        event_count: {
            type: Number
        },
        elected_isrc: {
            type: String
        },
        track_artist: {
            type: String
        },
        track_title: {
            type: String
        },
        isrcs: {
            type: String
        },
        "Label Name": {
            type: String
        },
        "Label ID": {
            type: String
        },
        repeated_event_count: {
            type: Number
        },
        event_count_1: {
            type: Number
        },
        "Total Revenue": {
            type: Number
        },
        "Sub Label Code": {
            type: String
        },
    },
    {
        timestamps: true,
        versionKey: false
    }
);

module.exports = mongoose.model("FacebookRevenue", FacebookRevenueSchema);
