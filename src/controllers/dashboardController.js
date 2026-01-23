const YoutubeDashboardSnapshot = require('../models/youtubeDashboardSnapshotModel');
const DashboardSnapshot = require('../models/dashboardSnapshotModel');
const TblReport2025 = require('../models/tblReport2025Model');
const Youtube = require('../models/youtubeModel');
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

    //getUnifiedAudioStreamingDashboard method
    async getUnifiedAudioStreamingDashboard(req, res) {
        try {
            const {
                labelId,
                search,
                fromDate,
                toDate
            } = req.query;

            const { role, userId } = req.user;

            const filter = {};

            if (role && role !== "Super Admin" && role !== "Manager") {
                const users = await User.find({ parent_id: userId }, { id: 1 }).lean();
                const childIds = users.map(u => u.id);
                childIds.push(userId);
                filter.user_id = { $in: childIds };
            }

            if (labelId) {
                filter.user_id = Number(labelId);
            }

            if (search && search.trim() !== "") {
                const regex = new RegExp(search.trim(), "i");

                filter.$or = [
                    { artist: regex },
                    { release: regex },
                    { track: regex }
                ];
            }

            if (fromDate && toDate) {
                const [fy, fm] = fromDate.split("-").map(Number);
                const [ty, tm] = toDate.split("-").map(Number);

                filter.date = {
                    $gte: new Date(fy, fm - 1, 1).toISOString().slice(0, 10),
                    $lte: new Date(ty, tm, 0).toISOString().slice(0, 10)
                };
            }

            const today = new Date();
            const currentYear = today.getFullYear();
            const currentMonthIndex = today.getMonth();
            const prevYear = currentYear - 1;

            const last12Start = new Date(today);
            last12Start.setMonth(today.getMonth() - 11);
            const last12StartStr = last12Start.toISOString().slice(0, 10);
            const todayStr = today.toISOString().slice(0, 10);

            const monthsLast12 = [];
            for (let i = 0; i < 12; i++) {
                const d = new Date(currentYear, currentMonthIndex - i, 1);
                monthsLast12.push(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`);
            }
            monthsLast12.reverse();

            const dayOfWeek = today.getDay();
            const startOfWeek = new Date(today);
            startOfWeek.setDate(today.getDate() - (dayOfWeek === 0 ? 6 : dayOfWeek - 1));
            startOfWeek.setHours(0, 0, 0, 0);
            const endOfWeek = new Date(startOfWeek);
            endOfWeek.setDate(startOfWeek.getDate() + 6);
            endOfWeek.setHours(23, 59, 59, 999);
            const weekStartStr = startOfWeek.toISOString().slice(0, 10);
            const weekEndStr = endOfWeek.toISOString().slice(0, 10);

            const facetResult = await TblReport2025.aggregate([
                { $match: filter },
                {
                    $facet: {
                        overview: [
                            {
                                $group: {
                                    _id: null,
                                    totalRevenue: { $sum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } } },
                                    totalStreams: { $sum: 1 },
                                    totalTrackReports: {
                                        $sum: {
                                            $convert: {
                                                input: "$track_count",
                                                to: "double",
                                                onError: 0,
                                                onNull: 0
                                            }
                                        }
                                    }
                                }
                            }
                        ],

                        topRelease: [
                            { $group: { _id: "$release", revenue: { $sum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } } } } },
                            { $sort: { revenue: -1 } },
                            { $limit: 1 },
                            { $project: { release: "$_id", revenue: { $round: ["$revenue", 2] }, _id: 0 } }
                        ],

                        topTrack: [
                            {
                                $group: {
                                    _id: { isrc: "$isrc_code", title: "$track_title", release: "$release", platform: "$retailer" },
                                    revenue: { $sum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } } },
                                    plays: { $sum: 1 }
                                }
                            },
                            { $sort: { revenue: -1 } },
                            { $limit: 1 },
                            {
                                $project: {
                                    track: "$_id.title",
                                    release: "$_id.release",
                                    platform: "$_id.platform",
                                    isrc: "$_id.isrc",
                                    revenue: { $round: ["$revenue", 2] },
                                    plays: 1,
                                    _id: 0
                                }
                            }
                        ],

                        topArtist: [
                            {
                                $group: {
                                    _id: "$track_artist",
                                    revenue: { $sum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } } },
                                    tracks: { $sum: 1 }
                                }
                            },
                            { $sort: { revenue: -1 } },
                            { $limit: 1 },
                            { $project: { artist: "$_id", revenue: { $round: ["$revenue", 2] }, tracks: 1, _id: 0 } }
                        ],

                        platforms: [
                            { $group: { _id: "$retailer", revenue: { $sum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } } } } },
                            { $sort: { revenue: -1 } },
                            { $project: { platform: { $ifNull: ["$_id", "Unknown"] }, value: { $round: ["$revenue", 2] }, _id: 0 } }
                        ],

                        territories: [
                            { $group: { _id: "$territory", value: { $sum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } } } } },
                            { $sort: { value: -1 } },
                            { $project: { territory: { $ifNull: ["$_id", "Unknown"] }, value: { $round: ["$value", 2] }, _id: 0 } }
                        ]
                    }
                }
            ]);

            const f = facetResult[0];

            const monthlyRevAgg = await TblReport2025.aggregate([
                { $match: filter },
                { $match: { $expr: { $and: [{ $gte: ["$date", last12StartStr] }, { $lte: ["$date", todayStr] }] } } },
                { $addFields: { month: { $substr: ["$date", 0, 7] } } },
                { $group: { _id: "$month", revenue: { $sum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } } } } },
                { $sort: { _id: 1 } }
            ]);

            const revMap = new Map(monthlyRevAgg.map(r => [r._id, r.revenue]));
            const monthlyRevenue = monthsLast12.map(m => ({
                month: m,
                revenue: revMap.get(m) ?? 0
            }));

            const stackedAgg = await TblReport2025.aggregate([
                { $match: filter },
                { $match: { $expr: { $and: [{ $gte: ["$date", last12StartStr] }, { $lte: ["$date", todayStr] }] } } },
                { $addFields: { month: { $substr: ["$date", 0, 7] } } },
                { $group: { _id: { month: "$month", retailer: "$retailer" }, revenue: { $sum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } } } } },
                { $group: { _id: "$_id.month", platforms: { $push: { name: { $ifNull: ["$_id.retailer", "Unknown"] }, value: "$revenue" } } } },
                { $sort: { _id: 1 } }
            ]);

            const stackedMap = new Map(stackedAgg.map(r => [r._id, r.platforms]));
            const monthlyStacked = monthsLast12.map(m => ({
                _id: m,
                platforms: stackedMap.get(m) ?? []
            }));

            const weeklyAgg = await TblReport2025.aggregate([
                { $match: filter },
                { $match: { $expr: { $and: [{ $gte: ["$date", weekStartStr] }, { $lte: ["$date", weekEndStr] }] } } },
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
                                    { case: { $eq: [{ $dayOfWeek: { $toDate: "$date" } }, 7] }, then: "Sun" },
                                ],
                                default: "Unknown"
                            }
                        }
                    }
                },
                { $group: { _id: "$weekday", streams: { $sum: 1 } } }
            ]);

            const weekdayOrder = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
            const weeklyMap = new Map(weeklyAgg.map(d => [d._id, d.streams]));
            const weeklyStreams = weekdayOrder.map(day => ({
                day,
                streams: weeklyMap.get(day) ?? 0
            }));

            const yoyAgg = await TblReport2025.aggregate([
                { $match: filter },
                {
                    $addFields: {
                        year: {
                            $cond: [
                                { $or: [{ $eq: ["$date", null] }, { $eq: ["$date", ""] }] },
                                null,
                                { $substr: ["$date", 0, 4] }
                            ]
                        },

                        monthNum: {
                            $cond: [
                                {
                                    $or: [
                                        { $eq: ["$date", null] },
                                        { $eq: ["$date", ""] }
                                    ]
                                },
                                null,
                                {
                                    $toInt: {
                                        $substr: ["$date", 5, 2]
                                    }
                                }
                            ]
                        }
                    }
                },

                {
                    $match: {
                        $expr: {
                            $and: [
                                { $in: ["$year", [prevYear.toString(), currentYear.toString()]] },
                                { $gte: ["$monthNum", 1] },
                                { $lte: ["$monthNum", 12] }
                            ]
                        }
                    }
                },

                {
                    $group: {
                        _id: {
                            year: "$year",
                            month: "$monthNum"
                        },
                        streams: { $sum: 1 }
                    }
                },

                { $sort: { "_id.year": 1, "_id.month": 1 } }
            ]);


            const prevMap = {}; const currMap = {};
            yoyAgg.forEach(item => {
                const y = item._id.year;
                const m = item._id.month;
                if (y === prevYear.toString()) prevMap[m] = item.streams;
                else if (y === currentYear.toString()) currMap[m] = item.streams;
            });

            const labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
            const prevStreams = labels.map((_, i) => prevMap[i + 1] ?? 0);
            const currStreams = labels.map((_, i) => (i <= currentMonthIndex) ? (currMap[i + 1] ?? 0) : 0);

            const yearlyAgg = await TblReport2025.aggregate([
                { $match: filter },
                {
                    $addFields: {
                        year: { $substr: ["$date", 0, 4] }
                    }
                },

                {
                    $group: {
                        _id: "$year",
                        streams: { $sum: 1 },

                        revenue: {
                            $sum: {
                                $toDouble: {
                                    $cond: [
                                        {
                                            $or: [
                                                { $eq: ["$net_total", ""] },
                                                { $eq: ["$net_total", null] }
                                            ]
                                        },
                                        0,
                                        "$net_total"
                                    ]
                                }
                            }
                        }
                    }
                },

                {
                    $project: {
                        year: "$_id",
                        streams: 1,
                        revenue: { $round: ["$revenue", 2] },
                        _id: 0
                    }
                },

                { $sort: { year: 1 } }
            ]);


            let yearlyGrowth = null;
            if (yearlyAgg.length >= 2) {
                const current = yearlyAgg[yearlyAgg.length - 1];
                const previous = yearlyAgg[yearlyAgg.length - 2];
                const growth = previous.streams > 0 ? ((current.streams - previous.streams) / previous.streams) * 100 : 0;
                yearlyGrowth = {
                    currentYear: current.year,
                    currentStreams: current.streams,
                    previousStreams: previous.streams,
                    growthPercentage: growth.toFixed(1)
                };
            }

            const trendsAgg = await TblReport2025.aggregate([
                { $match: filter },
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $gte: ["$date", last12StartStr] },
                                { $lte: ["$date", todayStr] }
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
                        streams: { $sum: 1 }
                    }
                },
                {
                    $group: {
                        _id: "$_id.month",
                        distributors: {
                            $push: {
                                name: { $ifNull: ["$_id.retailer", "Unknown"] },
                                streams: "$streams"
                            }
                        }
                    }
                },
                { $sort: { _id: 1 } }
            ]);

            const allRetailers = new Set();
            trendsAgg.forEach(item => {
                item.distributors.forEach(d => {
                    if (d.name) allRetailers.add(d.name);
                });
            });

            const retailerList = Array.from(allRetailers).sort();
            const filterOptions = ["All", ...retailerList];
            const monthMap = new Map(trendsAgg.map(item => [item._id, item.distributors]));

            const monthlyTrendsData = monthsLast12.map(monthKey => {
                const row = { month: monthKey, all: 0 };
                retailerList.forEach(r => row[r] = 0);

                const distributors = monthMap.get(monthKey) || [];
                distributors.forEach(d => {
                    if (d.name) {
                        row[d.name] = d.streams || 0;
                        row.all += d.streams || 0;
                    }
                });

                const [y, m] = monthKey.split('-');
                const dateObj = new Date(Number(y), Number(m) - 1, 1);
                row.displayMonth = dateObj.toLocaleString('default', { month: 'short', year: 'numeric' });

                return row;
            });

            const trendsPeriod = monthlyTrendsData.length > 0
                ? `${monthlyTrendsData[0].displayMonth} – ${monthlyTrendsData[monthlyTrendsData.length - 1].displayMonth}`
                : "";

            const yearlyStreams = [
                {
                    data: yearlyAgg.map(y => ({
                        year: y.year,
                        streams: y.streams,
                        revenue: y.revenue,
                        tracks: y.streams
                    })),
                    summary: null
                }
            ];


            return res.json({
                success: true,
                data: {
                    overview: {
                        totalRevenue: (f.overview[0]?.totalRevenue || 0).toFixed(2),
                        totalStreams: f.overview[0]?.totalStreams || 0,
                        topRelease: f.topRelease[0] || null,
                        topTrack: f.topTrack[0] ? {
                            ...f.topTrack[0],
                            totalPlays: f.topTrack[0].plays || 0
                        } : null,
                        topArtist: f.topArtist[0] || null,
                    },
                    monthlyRevenue: monthlyRevenue,
                    platformShare: f.platforms || [],
                    revenueByMonthPlatform: monthlyStacked,
                    territoryRevenue: f.territories || [],
                    weeklyStreams: weeklyStreams,
                    yearlyStreams,
                    musicStreamComparison: [{
                        labels: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
                        previousYear: {
                            year: prevYear,
                            streams: prevStreams
                        },
                        currentYear: {
                            year: currentYear,
                            streams: currStreams
                        },
                        currentMonth: today.toLocaleString('default', { month: 'long' })
                    }],
                    streamingTrends: [{
                        months: monthlyTrendsData.map(row => row.displayMonth),
                        monthlyData: monthlyTrendsData,
                        distributors: filterOptions,
                        period: trendsPeriod
                    }]
                }

            });

        } catch (error) {
            console.error("Error in getUnifiedRevenueDashboard:", error);
            return res.status(500).json({
                success: false,
                message: "Internal server error",
                error: error.message
            });
        }
    }

    //getUnifiedYoutubeDashboard method
    async getUnifiedYoutubeDashboard(req, res) {
        try {
            const { labelId, search, fromDate, toDate } = req.query;
            const { role, userId } = req.user;

            const filter = {};

            if (role && role !== "Super Admin" && role !== "Manager") {
                const users = await User.find({ parent_id: userId }, { id: 1 }).lean();
                const childIds = users.map(u => u.id);
                childIds.push(userId);
                filter.user_id = { $in: childIds };
            }

            if (labelId) {
                filter.user_id = Number(labelId);
            }

            if (search && search.trim() !== "") {
                const regex = new RegExp(search.trim(), "i");
                filter.$or = [
                    { track_artist: regex },
                    { asset_title: regex }
                ];
            }

            if (fromDate && toDate) {
                const [fy, fm] = fromDate.split("-").map(Number);
                const [ty, tm] = toDate.split("-").map(Number);
                filter.date = {
                    $gte: new Date(fy, fm - 1, 1).toISOString().slice(0, 10),
                    $lte: new Date(ty, tm, 0).toISOString().slice(0, 10)
                };
            }

            const today = new Date();
            const currentYear = today.getFullYear();
            const currentMonthIndex = today.getMonth();
            const prevYear = currentYear - 1;

            const last12Start = new Date(today);
            last12Start.setMonth(today.getMonth() - 11);
            const last12StartStr = last12Start.toISOString().slice(0, 10);
            const todayStr = today.toISOString().slice(0, 10);

            const monthsLast12 = [];
            for (let i = 0; i < 12; i++) {
                const d = new Date(currentYear, currentMonthIndex - i, 1);
                monthsLast12.push(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`);
            }
            monthsLast12.reverse();

            const dayOfWeek = today.getDay();
            const startOfWeek = new Date(today);
            startOfWeek.setDate(today.getDate() - (dayOfWeek === 0 ? 6 : dayOfWeek - 1));
            startOfWeek.setHours(0, 0, 0, 0);
            const endOfWeek = new Date(startOfWeek);
            endOfWeek.setDate(startOfWeek.getDate() + 6);
            endOfWeek.setHours(23, 59, 59, 999);
            const weekStartStr = startOfWeek.toISOString().slice(0, 10);
            const weekEndStr = endOfWeek.toISOString().slice(0, 10);

            const facetResult = await Youtube.aggregate([
                { $match: filter },
                {
                    $facet: {
                        overview: [
                            {
                                $group: {
                                    _id: null,
                                    totalRevenue: { $sum: { $toDouble: "$total_revenue" } },
                                    totalStreams: { $sum: { $toDouble: "$total_play" } }
                                }
                            }
                        ],
                        topRelease: [
                            {
                                $group: {
                                    _id: "$asset_title",
                                    revenue: { $sum: { $toDouble: "$total_revenue" } }
                                }
                            },
                            { $sort: { revenue: -1 } },
                            { $limit: 1 },
                            { $project: { release: "$_id", revenue: { $round: ["$revenue", 2] }, _id: 0 } }
                        ],
                        topTrack: [
                            {
                                $group: {
                                    _id: {
                                        title: "$asset_title",
                                        release: "$asset_title",
                                        platform: "$retailer",
                                        isrc: "$isrc_code"
                                    },
                                    revenue: { $sum: { $toDouble: "$total_revenue" } },
                                    plays: { $sum: 1 }
                                }
                            },
                            { $sort: { revenue: -1 } },
                            { $limit: 1 },
                            {
                                $project: {
                                    track: "$_id.title",
                                    release: "$_id.release",
                                    platform: "$_id.platform",
                                    isrc: "$_id.isrc",
                                    revenue: { $round: ["$revenue", 2] },
                                    totalPlays: "$plays",
                                    _id: 0
                                }
                            }
                        ],
                        topArtist: [
                            {
                                $group: {
                                    _id: "$track_artist",
                                    revenue: { $sum: { $toDouble: "$total_revenue" } },
                                    tracks: { $sum: 1 }
                                }
                            },
                            { $sort: { revenue: -1 } },
                            { $limit: 1 },
                            {
                                $project: {
                                    artist: "$_id",
                                    revenue: { $round: ["$revenue", 2] },
                                    tracks: 1,
                                    _id: 0
                                }
                            }
                        ],
                        platforms: [
                            {
                                $group: {
                                    _id: "$retailer",
                                    revenue: { $sum: { $toDouble: "$total_revenue" } }
                                }
                            },
                            { $sort: { revenue: -1 } },
                            {
                                $project: {
                                    platform: { $ifNull: ["$_id", "Unknown"] },
                                    value: { $round: ["$revenue", 2] },
                                    _id: 0
                                }
                            }
                        ],
                        territories: [
                            {
                                $group: {
                                    _id: "$country",
                                    value: { $sum: { $toDouble: "$total_revenue" } }
                                }
                            },
                            { $sort: { value: -1 } },
                            {
                                $project: {
                                    territory: { $ifNull: ["$_id", "Unknown"] },
                                    value: { $round: ["$value", 2] },
                                    _id: 0
                                }
                            }
                        ]
                    }
                }
            ]);

            const f = facetResult[0];

            const monthlyRevAgg = await Youtube.aggregate([
                { $match: filter },
                { $match: { $expr: { $and: [{ $gte: ["$date", last12StartStr] }, { $lte: ["$date", todayStr] }] } } },
                { $addFields: { month: { $substr: ["$date", 0, 7] } } },
                { $group: { _id: "$month", revenue: { $sum: { $toDouble: "$total_revenue" } } } },
                { $sort: { _id: 1 } }
            ]);

            const revMap = new Map(monthlyRevAgg.map(r => [r._id, r.revenue]));
            const monthlyRevenue = monthsLast12.map(m => ({
                month: m,
                revenue: revMap.get(m) ?? 0
            }));

            const stackedAgg = await Youtube.aggregate([
                { $match: filter },
                { $match: { $expr: { $and: [{ $gte: ["$date", last12StartStr] }, { $lte: ["$date", todayStr] }] } } },
                { $addFields: { month: { $substr: ["$date", 0, 7] } } },
                { $group: { _id: { month: "$month", retailer: "$retailer" }, revenue: { $sum: { $toDouble: "$total_revenue" } } } },
                { $group: { _id: "$_id.month", platforms: { $push: { name: { $ifNull: ["$_id.retailer", "Unknown"] }, value: "$revenue" } } } },
                { $sort: { _id: 1 } }
            ]);

            const stackedMap = new Map(stackedAgg.map(r => [r._id, r.platforms]));
            const revenueByMonthPlatform = monthsLast12.map(m => ({
                _id: m,
                platforms: stackedMap.get(m) ?? []
            }));

            const weeklyAgg = await Youtube.aggregate([
                { $match: filter },
                { $match: { $expr: { $and: [{ $gte: ["$date", weekStartStr] }, { $lte: ["$date", weekEndStr] }] } } },
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
                                    { case: { $eq: [{ $dayOfWeek: { $toDate: "$date" } }, 7] }, then: "Sun" },
                                ],
                                default: "Unknown"
                            }
                        }
                    }
                },
                { $group: { _id: "$weekday", streams: { $sum: 1 } } }
            ]);

            const weekdayOrder = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
            const weeklyMap = new Map(weeklyAgg.map(d => [d._id, d.streams]));
            const weeklyStreams = weekdayOrder.map(day => ({
                day,
                streams: weeklyMap.get(day) ?? 0
            }));

            const yoyAgg = await Youtube.aggregate([
                { $match: filter },
                {
                    $addFields: {
                        year: { $substr: ["$date", 0, 4] },
                        monthNum: { $toInt: { $substr: ["$date", 5, 2] } }
                    }
                },
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $in: ["$year", [prevYear.toString(), currentYear.toString()]] },
                                { $gte: ["$monthNum", 1] },
                                { $lte: ["$monthNum", 12] }
                            ]
                        }
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

            const prevMap = {}; const currMap = {};
            yoyAgg.forEach(item => {
                const y = item._id.year;
                const m = item._id.month;
                if (y === prevYear.toString()) prevMap[m] = item.streams;
                else if (y === currentYear.toString()) currMap[m] = item.streams;
            });

            const labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
            const prevStreams = labels.map((_, i) => prevMap[i + 1] ?? 0);
            const currStreams = labels.map((_, i) => (i <= currentMonthIndex) ? (currMap[i + 1] ?? 0) : 0);

            const yearlyAgg = await Youtube.aggregate([
                { $match: filter },
                { $addFields: { year: { $substr: ["$date", 0, 4] } } },
                {
                    $group: {
                        _id: "$year",
                        streams: { $sum: 1 },
                        revenue: { $sum: { $toDouble: "$total_revenue" } }
                    }
                },
                {
                    $project: {
                        year: "$_id",
                        streams: 1,
                        revenue: { $round: ["$revenue", 2] },
                        _id: 0
                    }
                },
                { $sort: { year: 1 } }
            ]);

            const yearlyStreams = [{
                data: yearlyAgg,
                summary: null
            }];

            const trendsAgg = await Youtube.aggregate([
                { $match: filter },
                { $match: { $expr: { $and: [{ $gte: ["$date", last12StartStr] }, { $lte: ["$date", todayStr] }] } } },
                { $addFields: { month: { $substr: ["$date", 0, 7] } } },
                {
                    $group: {
                        _id: { month: "$month", retailer: "$retailer" },
                        streams: { $sum: 1 }
                    }
                },
                {
                    $group: {
                        _id: "$_id.month",
                        distributors: { $push: { name: { $ifNull: ["$_id.retailer", "Unknown"] }, streams: "$streams" } }
                    }
                },
                { $sort: { _id: 1 } }
            ]);

            const allRetailers = new Set();
            trendsAgg.forEach(item => {
                item.distributors.forEach(d => {
                    if (d.name) allRetailers.add(d.name);
                });
            });

            const retailerList = Array.from(allRetailers).sort();
            const filterOptions = ["All", ...retailerList];
            const monthMap = new Map(trendsAgg.map(item => [item._id, item.distributors]));

            const monthlyTrendsData = monthsLast12.map(monthKey => {
                const row = { month: monthKey, all: 0 };
                retailerList.forEach(r => row[r] = 0);
                const distributors = monthMap.get(monthKey) || [];
                distributors.forEach(d => {
                    if (d.name) {
                        row[d.name] = d.streams || 0;
                        row.all += d.streams || 0;
                    }
                });
                const [y, m] = monthKey.split('-');
                const dateObj = new Date(Number(y), Number(m) - 1, 1);
                row.displayMonth = dateObj.toLocaleString('default', { month: 'short', year: 'numeric' });
                return row;
            });

            const trendsPeriod = monthlyTrendsData.length > 0
                ? `${monthlyTrendsData[0].displayMonth} – ${monthlyTrendsData[monthlyTrendsData.length - 1].displayMonth}`
                : "";

            return res.json({
                success: true,
                data: {
                    overview: {
                        totalRevenue: (f.overview[0]?.totalRevenue || 0).toFixed(2),
                        totalStreams: f.overview[0]?.totalStreams || 0,
                        topRelease: f.topRelease[0] || null,
                        topTrack: f.topTrack[0] || null,
                        topArtist: f.topArtist[0] || null,
                    },
                    monthlyRevenue,
                    platformShare: f.platforms || [],
                    revenueByMonthPlatform,
                    territoryRevenue: f.territories || [],
                    weeklyStreams,
                    yearlyStreams,
                    musicStreamComparison: [{
                        labels,
                        previousYear: { year: prevYear, streams: prevStreams },
                        currentYear: { year: currentYear, streams: currStreams },
                        currentMonth: today.toLocaleString('default', { month: 'long' })
                    }],
                    streamingTrends: [{
                        months: monthlyTrendsData.map(row => row.displayMonth),
                        monthlyData: monthlyTrendsData,
                        distributors: filterOptions,
                        period: trendsPeriod
                    }]
                }
            });

        } catch (error) {
            console.error("Error in getUnifiedYoutubeDashboard:", error);
            return res.status(500).json({
                success: false,
                message: "Internal server error",
                error: error.message
            });
        }
    }
}

module.exports = new DashboardController();