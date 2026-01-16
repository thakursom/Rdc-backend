// models/dashboardSnapshotModel.js
const mongoose = require('mongoose');

const dashboardSnapshotSchema = new mongoose.Schema({
    user_id: {
        type: Number,
        ref: "User",
        default: null
    },
    overview: {
        type: Object,
        default: {}
    },
    monthlyRevenue: {
        type: Array,
        default: []
    },
    platformShare: {
        type: Array,
        default: []
    },
    revenueByMonthPlatform: {
        type: Array,
        default: []
    },
    territoryRevenue: {
        type: Array,
        default: []
    },
    yearlyStreams: {
        type: Array,
        default: []
    },
    weeklyStreams: {
        type: Array,
        default: []
    },
    musicStreamComparison: {
        type: Array,
        default: []
    },
    streamingTrends: {
        type: Array,
        default: []
    },
}, {
    timestamps: true,
    versionKey: false
});

dashboardSnapshotSchema.index({ user_id: 1, type: 1 });

module.exports = mongoose.model('DashboardSnapshot', dashboardSnapshotSchema);