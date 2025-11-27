const mongoose = require("mongoose");

const JioSaavanRevenueSchema = new mongoose.Schema({
    user_id: {
        type: Number,
        ref: "User",
        default: null
    },
    uploadId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "RevenueUpload",
        required: true
    },
    retailer: {
        type: String,
        default: null
    },
    label: {
        type: String,
        default: null
    },
    upc_code: {
        type: String,
        default: null
    },
    catalogue_number: {
        type: String,
        default: null
    },
    isrc_code: {
        type: String,
        default: null
    },
    release: {
        type: String,
        default: null
    },
    track_title: {
        type: String,
        default: null
    },
    track_artist: {
        type: String,
        default: null
    },
    remixer_name: {
        type: String,
        default: null
    },
    remix: {
        type: String,
        default: null
    },
    territory: {
        type: String,
        default: null
    },
    purchase_status: {
        type: String,
        default: null
    },
    format: {
        type: String,
        default: null
    },
    delivery: {
        type: String,
        default: null
    },
    content_type: {
        type: String,
        default: null
    },
    track_count: {
        type: String,
        default: null
    },
    sale_type: {
        type: String,
        default: null
    },
    net_total: {
        type: String,
        default: null
    },
    date: {
        type: String,
        default: null
    },
    uploading_date: {
        type: String,
        default: null
    }
},
    {
        timestamps: true,
        versionKey: false
    });

module.exports = mongoose.model("JioSaavanRevenue", JioSaavanRevenueSchema);
