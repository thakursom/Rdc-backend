const YoutubeDashboardSnapshot = require('../models/youtubeDashboardSnapshotModel');
const DashboardSnapshot = require('../models/dashboardSnapshotModel');
const TblReport2025 = require('../models/tblReport2025Model');
const User = require('../models/userModel');


const buildMatchStage = async (req) => {
    const { role, userId } = req.user || {};

    if (!role || role === "Super Admin" || role === "Manager") {
        return {};
    }

    try {
        const users = await User.find({ parent_id: userId }, { _id: 1 }).lean();
        const childIds = users.map(u => u._id);
        childIds.push(userId);
        return { user_id: { $in: childIds } };
    } catch (err) {
        console.error("Error in getUserFilter:", err);
        return { user_id: userId };
    }
};

class DashboardController {
    constructor() { }

    //getRevenueDashboard method
    async getRevenueDashboard(req, res, next) {
        try {
            const match = await buildMatchStage(req);
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
                        totalStream: [
                            {
                                $group: {
                                    _id: null,
                                    totalStream: { $sum: { $toDouble: "$track_count" } }
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
                    totalRevenue: data.totalRevenue[0]?.totalRevenue.toFixed(2) || 0,
                    totalStream: data.totalStream[0]?.totalStream || 0,
                    topRelease: data.topRelease[0]
                        ? {
                            release: data.topRelease[0]._id,
                            revenue: data.topRelease[0].totalRevenue.toFixed(2) || 0
                        }
                        : null,
                    topTrack: data.topTracks[0]
                        ? {
                            track: data.topTracks[0].track,
                            revenue: data.topTracks[0].revenue.toFixed(2) || 0
                        }
                        : null,
                    topArtist: data.topArtists[0]
                        ? {
                            artist: data.topArtists[0].artist,
                            revenue: data.topArtists[0].revenue.toFixed(2) || 0
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
            const match = await buildMatchStage(req);

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
            const match = await buildMatchStage(req);

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
            const match = await buildMatchStage(req);
            const today = new Date();
            const startDate = new Date(today);
            startDate.setMonth(today.getMonth() - 11);

            const startStr = startDate.toISOString().slice(0, 10);
            const endStr = today.toISOString().slice(0, 10);

            const monthsList = [];
            for (let i = 0; i < 12; i++) {
                const d = new Date(startDate.getFullYear(), startDate.getMonth() + i, 1);
                const year = d.getFullYear();
                const month = String(d.getMonth() + 1).padStart(2, '0');
                monthsList.push(`${year}-${month}`);
            }

            const result = await TblReport2025.aggregate([
                { $match: match },
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
                        month: { $substr: ["$date", 0, 7] }
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

            const revenueMap = new Map(result.map(item => [item._id, item.platforms]));
            const finalData = monthsList.map(month => ({
                _id: month,
                platforms: revenueMap.get(month) || []
            }));

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
            const match = await buildMatchStage(req);

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
            const match = await buildMatchStage(req);

            const result = await TblReport2025.aggregate([
                { $match: match },
                {
                    $addFields: {
                        year: { $substr: ["$date", 0, 4] }
                    }
                },
                {
                    $group: {
                        _id: "$year",
                        streams: { $sum: 1 },
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
                { $sort: { year: 1 } }
            ]);
            const currentYear = new Date().getFullYear().toString();
            const currentYearData = result.find(r => r.year === currentYear);

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
                summary: growthInfo
            });
        } catch (error) {
            console.error('Error in getYearlyStreams:', error);
            return res.status(500).json({
                success: false,
                message: 'Internal server error'
            });
        }
    }

    //getWeeklyStreams method
    async getWeeklyStreams(req, res) {
        try {
            const match = await buildMatchStage(req);

            const today = new Date();
            const dayOfWeek = today.getDay();
            const startOfWeek = new Date(today);
            startOfWeek.setDate(today.getDate() - (dayOfWeek === 0 ? 6 : dayOfWeek - 1));
            startOfWeek.setHours(0, 0, 0, 0);

            const endOfWeek = new Date(startOfWeek);
            endOfWeek.setDate(startOfWeek.getDate() + 6);
            endOfWeek.setHours(23, 59, 59, 999);

            const startStr = startOfWeek.toISOString().slice(0, 10);
            const endStr = endOfWeek.toISOString().slice(0, 10);

            const result = await TblReport2025.aggregate([
                { $match: match },
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
                        weekday: {
                            $switch: {
                                branches: [
                                    { case: { $eq: [{ $dayOfWeek: { $toDate: "$date" } }, 1] }, then: "Mon" },
                                    { case: { $eq: [{ $dayOfWeek: { $toDate: "$date" } }, 2] }, then: "Tue" },
                                    { case: { $eq: [{ $dayOfWeek: { $toDate: "$date" } }, 3] }, then: "Wed" },
                                    { case: { $eq: [{ $dayOfWeek: { $toDate: "$date" } }, 4] }, then: "Thu" },
                                    { case: { $eq: [{ $dayOfWeek: { $toDate: "$date" } }, 5] }, then: "Fri" },
                                    { case: { $eq: [{ $dayOfWeek: { $toDate: "$date" } }, 6] }, then: "Sat" },
                                    { case: { $eq: [{ $dayOfWeek: { $toDate: "$date" } }, 7] }, then: "Sun" }
                                ],
                                default: "Unknown"
                            }
                        }
                    }
                },

                {
                    $group: {
                        _id: "$weekday",
                        streams: { $sum: 1 }
                    }
                }
            ]);

            const weekdayOrder = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
            const streamsMap = new Map(result.map(item => [item._id, item.streams]));

            const finalData = weekdayOrder.map(day => ({
                day,
                streams: streamsMap.get(day) || 0
            }));

            return res.status(200).json({
                success: true,
                data: finalData,
                weekRange: `${startOfWeek.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} - ${endOfWeek.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`
            });

        } catch (error) {
            console.error('Error in getWeeklyStreams:', error);
            return res.status(500).json({
                success: false,
                message: 'Internal server error'
            });
        }
    }

    //getMusicStreamComparison method
    async getMusicStreamComparison(req, res) {
        try {
            const match = await buildMatchStage(req);

            const today = new Date();
            const currentYear = today.getFullYear().toString();
            const prevYear = (today.getFullYear() - 1).toString();
            const currentMonthIndex = today.getMonth();

            const result = await TblReport2025.aggregate([
                { $match: match },
                {
                    $match: {
                        $expr: {
                            $in: [{ $substr: ["$date", 0, 4] }, [prevYear, currentYear]]
                        }
                    }
                },
                {
                    $addFields: {
                        year: { $substr: ["$date", 0, 4] },
                        monthNum: { $toInt: { $substr: ["$date", 5, 2] } }
                    }
                },
                {
                    $group: {
                        _id: { year: "$year", month: "$monthNum" },
                        streams: { $sum: 1 }
                    }
                },
                { $sort: { "_id.year": 1, "_id.month": 1 } }
            ]);

            const prevMap = {};
            const currMap = {};

            result.forEach(item => {
                const year = item._id.year;
                const month = item._id.month;
                const streams = item.streams;

                if (year === prevYear) {
                    prevMap[month] = streams;
                } else if (year === currentYear) {
                    currMap[month] = streams;
                }
            });

            const labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

            const prevStreams = labels.map((_, index) => {
                const monthNum = index + 1;
                return prevMap[monthNum] || 0;
            });

            const currStreams = labels.map((_, index) => {
                const monthNum = index + 1;
                if (index > currentMonthIndex) return 0;
                return currMap[monthNum] || 0;
            });

            return res.status(200).json({
                success: true,
                data: {
                    labels,
                    previousYear: {
                        year: prevYear,
                        streams: prevStreams
                    },
                    currentYear: {
                        year: currentYear,
                        streams: currStreams
                    }
                },
                currentMonth: today.toLocaleString('default', { month: 'long' })
            });

        } catch (error) {
            console.error('Error in getMusicStreamComparison:', error);
            return res.status(500).json({
                success: false,
                message: 'Internal server error'
            });
        }
    }

    //getStreamingTrendsOverTime method
    async getStreamingTrendsOverTime(req, res) {
        try {
            const match = await buildMatchStage(req);

            const today = new Date();
            const startDate = new Date(today);
            startDate.setMonth(today.getMonth() - 11);

            const startStr = startDate.toISOString().slice(0, 10);
            const endStr = today.toISOString().slice(0, 10);

            const result = await TblReport2025.aggregate([
                { $match: match },
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
                        month: { $substr: ["$date", 0, 7] }
                    }
                },
                {
                    $group: {
                        _id: {
                            month: "$month",
                            retailer: "$retailer"
                        },
                        streams: { $sum: 1 }
                    }
                },
                {
                    $group: {
                        _id: "$_id.month",
                        distributors: {
                            $push: {
                                name: "$_id.retailer",
                                streams: "$streams"
                            }
                        }
                    }
                },

                { $sort: { _id: 1 } }
            ]);

            const allRetailers = new Set();
            result.forEach(item => {
                item.distributors.forEach(d => {
                    if (d.name) allRetailers.add(d.name);
                });
            });

            const retailerList = Array.from(allRetailers).sort();
            const filterOptions = ["All", ...retailerList];

            const monthsList = [];
            let current = new Date(startDate);
            while (current <= today) {
                const year = current.getFullYear();
                const month = String(current.getMonth() + 1).padStart(2, '0');
                monthsList.push(`${year}-${month}`);
                current.setMonth(current.getMonth() + 1);
            }

            const monthMap = new Map(result.map(item => [item._id, item.distributors]));

            const monthlyData = monthsList.map((monthKey) => {
                const row = { month: monthKey, all: 0 };

                retailerList.forEach(retailer => {
                    row[retailer] = 0;
                });

                const distributors = monthMap.get(monthKey) || [];
                distributors.forEach(d => {
                    if (d.name) {
                        row[d.name] = d.streams || 0;
                        row.all += d.streams || 0;
                    }
                });

                const [y, m] = monthKey.split('-');
                const date = new Date(y, m - 1, 1);
                row.displayMonth = date.toLocaleString('default', { month: 'short', year: 'numeric' });

                return row;
            });

            return res.status(200).json({
                success: true,
                data: {
                    months: monthlyData.map(m => m.displayMonth),
                    monthlyData,
                    distributors: filterOptions
                },
                period: `${monthlyData[0]?.displayMonth} - ${monthlyData[monthlyData.length - 1]?.displayMonth}`
            });

        } catch (error) {
            console.error('Error in getStreamingTrendsOverTime:', error);
            return res.status(500).json({
                success: false,
                message: 'Internal server error'
            });
        }
    }

    //getAudioStreamingDashboard method
    async getAudioStreamingDashboard(req, res) {
        try {
            const { userId } = req.user;

            if (!userId) {
                return res.status(400).json({
                    success: false,
                    message: "User ID missing"
                });
            }

            const user = await User.findOne({ id: userId })
                .select('id role')
                .lean();

            if (!user) {
                return res.status(404).json({
                    success: false,
                    message: "User not found"
                });
            }

            const snapshot = await DashboardSnapshot.findOne({ user_id: userId })
                .select('-_id -createdAt -updatedAt')
                .lean();

            if (!snapshot) {
                return res.status(200).json({
                    success: true,
                    data: {
                        overview: {},
                        monthlyRevenue: [],
                        platformShare: [],
                        revenueByMonthPlatform: [],
                        territoryRevenue: [],
                        yearlyStreams: [],
                        weeklyStreams: [],
                        musicStreamComparison: [],
                        streamingTrends: []
                    }
                });
            }

            return res.status(200).json({
                success: true,
                data: {
                    overview: snapshot.overview ?? {},
                    monthlyRevenue: snapshot.monthlyRevenue ?? [],
                    platformShare: snapshot.platformShare ?? [],
                    revenueByMonthPlatform: snapshot.revenueByMonthPlatform ?? [],
                    territoryRevenue: snapshot.territoryRevenue ?? [],
                    yearlyStreams: snapshot.yearlyStreams ?? [],
                    weeklyStreams: snapshot.weeklyStreams ?? [],
                    musicStreamComparison: snapshot.musicStreamComparison ?? [],
                    streamingTrends: snapshot.streamingTrends ?? []
                }
            });

        } catch (error) {
            console.error("getAudioStreamingDashboard error:", error);
            return res.status(500).json({
                success: false,
                message: "Internal server error"
            });
        }
    }

    //getYoutubeDashboard method
    async getYoutubeDashboard(req, res) {
        try {
            const { userId } = req.user;

            if (!userId) {
                return res.status(400).json({
                    success: false,
                    message: "User ID missing"
                });
            }

            const user = await User.findOne({ id: userId })
                .select('id role')
                .lean();

            if (!user) {
                return res.status(404).json({
                    success: false,
                    message: "User not found"
                });
            }

            const snapshot = await YoutubeDashboardSnapshot.findOne({ user_id: userId })
                .select('-_id -createdAt -updatedAt')
                .lean();

            if (!snapshot) {
                return res.status(200).json({
                    success: true,
                    data: {
                        overview: {},
                        monthlyRevenue: [],
                        platformShare: [],
                        revenueByMonthPlatform: [],
                        territoryRevenue: [],
                        yearlyStreams: [],
                        weeklyStreams: [],
                        musicStreamComparison: [],
                        streamingTrends: []
                    }
                });
            }

            return res.status(200).json({
                success: true,
                data: {
                    overview: snapshot.overview ?? {},
                    monthlyRevenue: snapshot.monthlyRevenue ?? [],
                    platformShare: snapshot.platformShare ?? [],
                    revenueByMonthPlatform: snapshot.revenueByMonthPlatform ?? [],
                    territoryRevenue: snapshot.territoryRevenue ?? [],
                    yearlyStreams: snapshot.yearlyStreams ?? [],
                    weeklyStreams: snapshot.weeklyStreams ?? [],
                    musicStreamComparison: snapshot.musicStreamComparison ?? [],
                    streamingTrends: snapshot.streamingTrends ?? []
                }
            });

        } catch (error) {
            console.error("getAudioStreamingDashboard error:", error);
            return res.status(500).json({
                success: false,
                message: "Internal server error"
            });
        }
    }
}

module.exports = new DashboardController();