const mongoose = require("mongoose");

const PaymentHistorySchema = new mongoose.Schema({
    user_id: {
        type: Number,
        required: true
    },

    paymentMethod: {
        type: String,
        enum: ["bank", "paypal", "upi"],
        required: true
    },

    amount: {
        type: Number,
        required: true
    },

    totalAmount: {
        type: Number,
        required: true
    },

    description: {
        type: String,
        default: ""
    },
    paymentDetails: {
        type: Object,
        required: true
    }

}, {
    timestamps: true,
    versionKey: false
});

module.exports = mongoose.model("PaymentHistory", PaymentHistorySchema);
