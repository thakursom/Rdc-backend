const mongoose = require("mongoose");

const RevenueSummarySchema = new mongoose.Schema(
  {
    user_id: {
      type: Number,
      ref: "User",
      default: null
    },
    netRevenueByMonth: {
      type: Map,
      of: Number,
      default: {}
    },
    revenueByChannel: {
      type: Map,
      of: Number,
      default: {}
    },
    revenueByCountry: {
      type: Map,
      of: Number,
      default: {}
    }
  },
  {
    timestamps: true,
    versionKey: false
  }
);

module.exports = mongoose.model("RevenueSummary", RevenueSummarySchema);
