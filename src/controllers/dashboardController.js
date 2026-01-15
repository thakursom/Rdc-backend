const TblReport2025 = require('../models/tblReport2025Model');


const buildMatchStage = (query) => {
    const { startDate, endDate, label } = query;
    const match = {};

    if (startDate && endDate) {
        match.date = {
            $gte: startDate,
            $lte: endDate
        };
    }

    if (label && label !== 'label') {
        match.label = label;
    }

    return match;
};

class DashboardController {
    constructor() { }

    //getRevenueDashboard method
    async getRevenueDashboard(req, res, next) {
        try {
            const match = buildMatchStage(req.query);

            const result = await TblReport2025.aggregate([
                { $match: match },

                {
                    $facet: {

                        totalRevenue: [
                            {
                                $group: {
                                    _id: null,
                                    totalRevenue: { $sum: { $toDouble: "$net_total" } }
                                }
                            }
                        ],

                        topRelease: [
                            {
                                $group: {
                                    _id: "$release",
                                    totalRevenue: { $sum: { $toDouble: "$net_total" } }
                                }
                            },
                            { $sort: { totalRevenue: -1 } },
                            { $limit: 1 }
                        ],

                        topTracks: [
                            {
                                $group: {
                                    _id: {
                                        isrc: "$isrc_code",
                                        title: "$track_title",
                                        release: "$release",
                                        platform: "$retailer"
                                    },
                                    totalRevenue: { $sum: { $toDouble: "$net_total" } },
                                    totalPlays: { $sum: 1 }
                                }
                            },
                            { $sort: { totalRevenue: -1 } },
                            { $limit: 1 },
                            {
                                $project: {
                                    _id: 0,
                                    track: "$_id.title",
                                    release: "$_id.release",
                                    platform: "$_id.platform",
                                    isrc: "$_id.isrc",
                                    revenue: { $round: ["$totalRevenue", 2] },
                                    totalPlays: 1
                                }
                            }
                        ],

                        topArtists: [
                            {
                                $group: {
                                    _id: "$track_artist",
                                    totalRevenue: { $sum: { $toDouble: "$net_total" } },
                                    trackCount: { $sum: 1 }
                                }
                            },
                            { $sort: { totalRevenue: -1 } },
                            { $limit: 1 },
                            {
                                $project: {
                                    _id: 0,
                                    artist: "$_id",
                                    revenue: { $round: ["$totalRevenue", 2] },
                                    tracks: "$trackCount"
                                }
                            }
                        ]
                    }
                }
            ]);

            const data = result[0];

            return res.json({
                success: true,
                data: {
                    totalRevenue: data.totalRevenue[0]?.totalRevenue || 0,
                    topRelease: data.topRelease[0]
                        ? {
                            release: data.topRelease[0]._id,
                            revenue: data.topRelease[0].totalRevenue
                        }
                        : null,
                    topTrack: data.topTracks[0]
                        ? {
                            track: data.topTracks[0].track,
                            revenue: data.topTracks[0].revenue
                        }
                        : null,
                    topArtist: data.topArtists[0]
                        ? {
                            artist: data.topArtists[0].artist,
                            revenue: data.topArtists[0].revenue
                        }
                        : null,
                }
            });

        } catch (error) {
            console.error("Error in getRevenueDashboard:", error);
            return res.status(500).json({
                success: false,
                message: "Internal server error",
                error: error.message
            });
        }
    }

    //getMonthlyRevenue method
    async getMonthlyRevenue(req, res) {
        try {
            const match = buildMatchStage(req.query);

            const today = new Date();
            const currentYear = today.getFullYear();
            const currentMonth = today.getMonth();

            const monthsList = [];
            for (let i = 0; i < 12; i++) {
                const date = new Date(currentYear, currentMonth - i, 1);
                const year = date.getFullYear();
                const month = String(date.getMonth() + 1).padStart(2, '0');
                monthsList.push(`${year}-${month}`);
            }

            monthsList.reverse();

            const result = await TblReport2025.aggregate([
                { $match: match },
                {
                    $addFields: {
                        month: { $substr: ["$date", 0, 7] }
                    }
                },
                {
                    $group: {
                        _id: "$month",
                        totalRevenue: { $sum: { $toDouble: "$net_total" } }
                    }
                },
            ]);

            const revenueMap = new Map(
                result.map(item => [item._id, item.totalRevenue])
            );

            const finalData = monthsList.map(month => ({
                month,
                revenue: revenueMap.has(month) ? revenueMap.get(month) : 0
            }));

            const first = monthsList[0];
            const last = monthsList[monthsList.length - 1];
            const format = (m) => {
                const [y, mo] = m.split('-');
                return new Date(y, mo - 1, 1).toLocaleString('default', { month: 'short', year: 'numeric' });
            };

            return res.status(200).json({
                success: true,
                data: finalData,
                period: `${format(first)} - ${format(last)}`
            });

        } catch (error) {
            console.error('Error in getLast12MonthsRevenue:', error);
            return res.status(500).json({
                success: false,
                message: 'Internal server error'
            });
        }
    }

    //getPlatformShare method
    async getPlatformShare(req, res) {
        try {
            const match = buildMatchStage(req.query);

            const result = await TblReport2025.aggregate([
                { $match: match },
                {
                    $group: {
                        _id: "$retailer",
                        revenue: { $sum: { $toDouble: "$net_total" } }
                    }
                },
                { $sort: { revenue: -1 } }
            ]);

            const formatted = result.map(item => ({
                platform: item._id || "Unknown",
                value: Number(item.revenue.toFixed(2))
            }));

            return res.status(200).json({
                success: true,
                data: formatted
            });
        } catch (error) {
            console.error('Error in getPlatformShare:', error);
            return res.status(500).json({
                success: false,
                message: 'Internal server error'
            });
        }
    }

    //getRevenueByMonthStacked method
    async getRevenueByMonthStacked(req, res) {
        try {
            const match = buildMatchStage(req.query);

            // ── 1. Calculate the date range: last 12 months including current month ──
            const today = new Date();
            const startDate = new Date(today);
            startDate.setMonth(today.getMonth() - 11); // 12 months back

            const startStr = startDate.toISOString().slice(0, 10); // YYYY-MM-DD
            const endStr = today.toISOString().slice(0, 10);

            // ── 2. Generate all 12 month keys (YYYY-MM) in chronological order ──
            const monthsList = [];
            for (let i = 0; i < 12; i++) {
                const d = new Date(startDate.getFullYear(), startDate.getMonth() + i, 1);
                const year = d.getFullYear();
                const month = String(d.getMonth() + 1).padStart(2, '0');
                monthsList.push(`${year}-${month}`);
            }

            // ── 3. Aggregation ──
            const result = await TblReport2025.aggregate([
                { $match: match },

                // Filter only last 12 months
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $gte: ["$date", startStr] },
                                { $lte: ["$date", endStr] }
                            ]
                        }
                    }
                },

                {
                    $addFields: {
                        month: { $substr: ["$date", 0, 7] } // YYYY-MM
                    }
                },

                {
                    $group: {
                        _id: { month: "$month", retailer: "$retailer" },
                        revenue: { $sum: { $toDouble: "$net_total" } }
                    }
                },

                {
                    $group: {
                        _id: "$_id.month",
                        platforms: {
                            $push: { name: "$_id.retailer", value: "$revenue" }
                        }
                    }
                },

                { $sort: { _id: 1 } }
            ]);

            // ── 4. Create a map for quick lookup ──
            const revenueMap = new Map(result.map(item => [item._id, item.platforms]));

            // ── 5. Build final result with ALL 12 months (missing = empty array) ──
            const finalData = monthsList.map(month => ({
                _id: month,
                platforms: revenueMap.get(month) || []
            }));

            // Optional: human-readable period
            const format = (m) => {
                const [y, mo] = m.split('-');
                return new Date(y, mo - 1, 1).toLocaleString('default', { month: 'short', year: 'numeric' });
            };

            return res.status(200).json({
                success: true,
                data: finalData,
                period: `${format(monthsList[0])} - ${format(monthsList[monthsList.length - 1])}`
            });

        } catch (error) {
            console.error('Error in getLast12MonthsRevenueStacked:', error);
            return res.status(500).json({
                success: false,
                message: 'Internal server error'
            });
        }
    }

    //getTerritoryRevenue method
    async getTerritoryRevenue(req, res) {
        try {
            const match = buildMatchStage(req.query);

            const result = await TblReport2025.aggregate([
                { $match: match },
                {
                    $group: {
                        _id: "$territory",
                        revenue: { $sum: { $toDouble: "$net_total" } }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        territory: "$_id",
                        value: { $round: ["$revenue", 2] }
                    }
                },
                { $sort: { value: -1 } }
            ]);

            return res.status(200).json({
                success: true,
                data: result
            });
        } catch (error) {
            console.error('Error in getTerritoryRevenue:', error);
            return res.status(500).json({
                success: false,
                message: 'Internal server error'
            });
        }
    }

    //getYearlyStreams method
    async getYearlyStreams(req, res) {
        try {
            const match = buildMatchStage(req.query);

            const result = await TblReport2025.aggregate([
                { $match: match },
                {
                    $addFields: {
                        year: { $substr: ["$date", 0, 4] } // Extract YYYY from "YYYY-MM-DD"
                    }
                },
                {
                    $group: {
                        _id: "$year",
                        streams: { $sum: 1 },           // ← using row count as streams
                        // If you later add real streams field, change to:
                        // streams: { $sum: { $toDouble: "$streams" } },
                        revenue: { $sum: { $toDouble: "$net_total" } },
                        trackCount: { $sum: 1 }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        year: "$_id",
                        streams: 1,
                        revenue: { $round: ["$revenue", 2] },
                        tracks: "$trackCount"
                    }
                },
                { $sort: { year: 1 } } // chronological order
            ]);

            // Optional: add current year MTD info (if you want to show partial current year)
            const currentYear = new Date().getFullYear().toString();
            const currentYearData = result.find(r => r.year === currentYear);

            // You can also compute growth % on frontend, but here's a simple example
            let growthInfo = null;
            if (currentYearData && result.length >= 2) {
                const previousYear = result[result.length - 2];
                const growth = previousYear && previousYear.streams > 0
                    ? ((currentYearData.streams - previousYear.streams) / previousYear.streams) * 100
                    : 0;
                growthInfo = {
                    currentYearStreams: currentYearData.streams,
                    previousYearStreams: previousYear?.streams || 0,
                    growthPercentage: growth.toFixed(1)
                };
            }

            return res.status(200).json({
                success: true,
                data: result,
                summary: growthInfo // optional - helpful for card or title
            });
        } catch (error) {
            console.error('Error in getYearlyStreams:', error);
            return res.status(500).json({
                success: false,
                message: 'Internal server error'
            });
        }
    }

}

module.exports = new DashboardController();