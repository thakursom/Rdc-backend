const mongoose = require("mongoose");

const SpotifyRevenueSchema = new mongoose.Schema(
    {
        uploadId: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "RevenueUpload",
            required: true
        },
        Month: {
            type: String,
            required: false
        },
        Country: {
            type: String,
            required: false
        },
        Product: {
            type: String,
            required: false
        },
        URI: {
            type: String,
            required: false
        },
        UPC: {
            type: String,
            required: false
        },
        EAN: {
            type: String,
            required: false
        },
        ISRC: {
            type: String,
            required: false
        },
        "Track name": {
            type: String,
            required: false
        },
        "Artist name": {
            type: String,
            required: false
        },
        "Composer name": {
            type: String,
            required: false
        },
        "Album name": {
            type: String,
            required: false
        },
        "Label Name": {
            type: String,
            required: false
        },
        "Label Code": {
            type: String,
            required: false
        },
        Quantity: {
            type: Number,
            required: false
        },
        Total: {
            type: Number,
            required: false
        },
        "Sub Label Code": {
            type: String,
            required: false
        }
    },
    {
        timestamps: true,
        versionKey: false
    }
);

module.exports = mongoose.model("SpotifyRevenue", SpotifyRevenueSchema);
