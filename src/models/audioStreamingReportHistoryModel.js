const mongoose = require('mongoose');

const audioStreamingReportHistorySchema = new mongoose.Schema({
    filters: {
        type: Object,
        required: true
    },
    status: {
        type: String,
        enum: ['preparing', 'ready', 'failed'],
        default: 'preparing'
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