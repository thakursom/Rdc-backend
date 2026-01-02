const mongoose = require("mongoose");

const YTPremiumRevenueSchema = new mongoose.Schema({
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
    track_artist: {
        type: String,
        default: null
    },
    type: {
        type: String,
        default: null
    },
    asset_id: {
        type: String,
        default: null
    },
    country: {
        type: String,
        required: true
    },
    isrc_code: {
        type: String,
        default: null
    },
    upc_code: {
        type: String,
        default: null
    },
    sub_label_id: {
        type: String,
        default: null
    },
    sub_label_share: {
        type: String,
        default: null
    },
    partner_share: {
        type: String,
        default: null
    },
    content_type: {
        type: String,
        default: null
    },
    claim_type: {
        type: String,
        default: null
    },
    asset_title: {
        type: String,
        default: null
    },
    video_duration_sec: {
        type: String,
        default: null
    },
    category: {
        type: String,
        default: null
    },
    custom_id: {
        type: String,
        default: null
    },
    asset_channel_id: {
        type: String,
        default: null
    },
    channel_name: {
        type: String,
        default: null
    },
    label_name: {
        type: String,
        default: null
    },
    total_play: {
        type: Number,
        default: null
    },
    partner_revenue: {
        type: String,
        default: null
    },
    inr_rate: {
        type: Number,
        default: null
    },
    total_revenue: {
        type: Number,
        default: null
    },
    label_shared: {
        type: Number,
        default: null
    },
    added_date: {
        type: String,
        default: null
    },
    start_date: {
        type: String,
        default: null
    },
    end_date: {
        type: String,
        default: null
    },
    track_id: {
        type: String,
        default: null
    },
    album_id: {
        type: String,
        default: null
    },
    channel_type: {
        type: Number,
        default: 1
    },
    usd: {
        type: Number,
        default: null
    },
    usd_label_share: {
        type: Number,
        default: null
    },
    usd_rdc_share: {
        type: Number,
        default: null
    },
    label_share: {
        type: Number,
        default: null
    },
    rdc_share: {
        type: Number,
        default: null
    },
    fileid: {
        type: Number,
        default: null
    },
    status: {
        type: Number,
        default: 0
    },
    inv_generated: {
        type: Boolean,
        default: false
    },
    label_code: {
        type: String,
        default: null
    },
    video_link: {
        type: String,
        default: null
    },
    channel_link: {
        type: String,
        default: null
    },
    sub_label: {
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
}, {
    timestamps: true,
    versionKey: false
});

module.exports = mongoose.model("YTPremiumRevenue", YTPremiumRevenueSchema);
