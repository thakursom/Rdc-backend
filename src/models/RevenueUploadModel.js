const mongoose = require("mongoose");

const RevenueUploadSchema = new mongoose.Schema({
    user_id: {
        type: Number,
        ref: "User",
        default: null
    },
    platform: {
        type: String,
        required: true
    },
    fileName: {
        type: String
    },
    filePath: {
        type: String
    },
    fileExt: {
        type: String
    },
    periodFrom: {
        type: String,
        required: true
    },
    periodTo: {
        type: String,
        required: true
    },
    isAccepted: {
        type: Boolean,
        default: false
    },
}, {
    timestamps: true,
    versionKey: false
}
);

module.exports = mongoose.model("RevenueUpload", RevenueUploadSchema);
