const mongoose = require('mongoose');

const audioStreamingReportHistorySchema = new mongoose.Schema({
    user_id: {
        type: Number,
        ref: "User",
        default: null
    },
    filters: {
        type: Object,
        required: true
    },
    status: {
        type: String,
        enum: ['pending', 'ready', 'failed'],
        default: 'pending'
    },
    filename: {
        type: String
    },
    filePath: {
        type: String
    },
    fileURL: {
        type: String
    },
    generatedAt: {
        type: Date,
        default: Date.now
    },
    downloadedAt: {
        type: Date
    }
},
    {
        timestamps: true,
        versionKey: false
    });

module.exports = mongoose.model(' audioStreamingReportHistory', audioStreamingReportHistorySchema);