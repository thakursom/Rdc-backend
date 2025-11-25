const mongoose = require("mongoose");

const TikTokRevenueSchema = new mongoose.Schema(
    {
        uploadId: {
            type: mongoose.Schema.Types.ObjectId,
            ref: "RevenueUpload",
            required: true
        },
        Month: {
            type: String
        },
        report_start_date: {
            type: String
        },
        report_end_date: {
            type: String
        },
        platform_name: {
            type: String
        },
        Country: {
            type: String
        },
        provider_dpid: {
            type: String
        },
        platform_song_id: {
            type: String
        },
        song_title: {
            type: String
        },
        artist: {
            type: String
        },
        album: {
            type: String
        },
        label_name: {
            type: String
        },
        label_id: {
            type: String
        },
        isrc: {
            type: String
        },
        product_code: {
            type: String
        },
        video_views: {
            type: Number
        },
        genre: {
            type: String
        },
        content_type: {
            type: String
        },
        inr_revenue: {
            type: Number
        },
        sub_label_code: {
            type: String
        }
    },
    {
        timestamps: true,
        versionKey: false
    }
);

module.exports = mongoose.model("TikTokRevenue", TikTokRevenueSchema);
