const DashboardSnapshot = require('../models/dashboardSnapshotModel');
const TblReport2025 = require('../models/tblReport2025Model');
const User = require('../models/userModel');

class DashboardService {

    static async refreshAllAdminAndManagerDashboards() {
        try {
            const admins = await User.find({ role: { $in: ['Super Admin', 'Manager'] } }).lean();

            console.log(`[ADMIN REFRESH] Found ${admins.length} admins/managers`);

            for (const admin of admins) {

                try {
                    await this.calculateAndSaveDashboard(admin.id, admin.role);
                } catch (err) {
                    console.error(`Failed to refresh ${admin.role} ${admin.id}:`, err);
                }
            }

            console.log("[ADMIN REFRESH] Completed");
            return true;
        } catch (err) {
            console.error("[ADMIN REFRESH] Critical error:", err);
            return false;
        }
    }

    static async calculateAndSaveDashboard(userId, role) {
        try {
            let matchStage;
            let snapshotType;
            let snapshotUserId;

            if (role === 'Super Admin') {
                matchStage = {};
                snapshotType = 'global';
                snapshotUserId = userId;
            } else if (role === 'Manager') {
                matchStage = {};
                snapshotType = 'global';
                snapshotUserId = userId;
            } else {
                const users = await User.find({ parent_id: userId }, { _id: 1 }).lean();
                const childIds = users.map(u => u._id);
                childIds.push(userId);
                matchStage = { user_id: { $in: childIds } };
                snapshotType = 'personal';
                snapshotUserId = userId;
            }

            const [
                overview,
                monthlyRevenue,
                platformShare,
                revenueByMonthPlatform,
                territoryRevenue,
                yearlyStreams,
                weeklyStreams,
                musicStreamComparison,
                streamingTrends
            ] = await Promise.all([
                this.calculateOverview(matchStage),
                this.calculateMonthlyRevenue(matchStage),
                this.calculatePlatformShare(matchStage),
                this.calculateRevenueByMonthStacked(matchStage),
                this.calculateTerritoryRevenue(matchStage),
                this.calculateYearlyStreams(matchStage),
                this.calculateWeeklyStreams(matchStage),
                this.calculateMusicStreamComparison(matchStage),
                this.calculateStreamingTrendsOverTime(matchStage)
            ]);

            const snapshotData = {
                user_id: snapshotUserId,
                overview,
                monthlyRevenue,
                platformShare,
                revenueByMonthPlatform,
                territoryRevenue,
                yearlyStreams,
                weeklyStreams,
                musicStreamComparison,
                streamingTrends,
            };

            await DashboardSnapshot.updateOne(
                { user_id: snapshotUserId },
                {
                    $set: snapshotData,
                },
                { upsert: true }
            );

            console.log(` Dashboard saved → ${snapshotUserId}`);
            return true;
        } catch (err) {
            console.error(`Dashboard save failed for ${role} (${userId || 'global'}):`, err);
            return false;
        }
    }

    static async calculateOverview(match) {
        const [result] = await TblReport2025.aggregate([
            { $match: match },
            {
                $facet: {
                    totalRevenue: [{ $group: { _id: null, v: { $sum: { $toDouble: "$net_total" } } } }],
                    totalStream: [{ $group: { _id: null, v: { $sum: { $toDouble: "$track_count" } } } }],
                    topRelease: [
                        { $group: { _id: "$release", v: { $sum: { $toDouble: "$net_total" } } } },
                        { $sort: { v: -1 } }, { $limit: 1 }
                    ],
                    topTrack: [
                        {
                            $group: {
                                _id: { t: "$track_title", r: "$release", p: "$retailer", i: "$isrc_code" },
                                revenue: { $sum: { $toDouble: "$net_total" } },
                                plays: { $sum: 1 }
                            }
                        },
                        { $sort: { revenue: -1 } }, { $limit: 1 },
                        {
                            $project: {
                                _id: 0,
                                track: "$_id.t",
                                release: "$_id.r",
                                platform: "$_id.p",
                                isrc: "$_id.i",
                                revenue: { $round: ["$revenue", 2] },
                                totalPlays: "$plays"
                            }
                        }
                    ],
                    topArtist: [
                        {
                            $group: {
                                _id: "$track_artist",
                                revenue: { $sum: { $toDouble: "$net_total" } },
                                tracks: { $sum: 1 }
                            }
                        },
                        { $sort: { revenue: -1 } }, { $limit: 1 },
                        {
                            $project: {
                                _id: 0,
                                artist: "$_id",
                                revenue: { $round: ["$revenue", 2] },
                                tracks: 1
                            }
                        }
                    ]
                }
            }
        ]);

        const d = result || {};
        return {
            totalRevenue: (d.totalRevenue?.[0]?.v ?? 0).toFixed(2),
            totalStreams: d.totalStream?.[0]?.v ?? 0,
            topRelease: d.topRelease?.[0] ? { release: d.topRelease[0]._id, revenue: d.topRelease[0].v.toFixed(2) } : null,
            topTrack: d.topTrack?.[0] ?? null,
            topArtist: d.topArtist?.[0] ?? null
        };
    }

    static async calculateMonthlyRevenue(match) {
        const today = new Date();
        const months = [];
        for (let i = 0; i < 12; i++) {
            const d = new Date(today.getFullYear(), today.getMonth() - i, 1);
            months.push(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`);
        }
        months.reverse();

        const agg = await TblReport2025.aggregate([
            { $match: match },
            { $addFields: { m: { $substr: ["$date", 0, 7] } } },
            { $group: { _id: "$m", revenue: { $sum: { $toDouble: "$net_total" } } } }
        ]);

        const map = new Map(agg.map(x => [x._id, x.revenue]));
        return months.map(m => ({ month: m, revenue: map.get(m) ?? 0 }));
    }

    static async calculatePlatformShare(match) {
        const agg = await TblReport2025.aggregate([
            { $match: match },
            { $group: { _id: "$retailer", v: { $sum: { $toDouble: "$net_total" } } } },
            { $sort: { v: -1 } }
        ]);

        return agg.map(x => ({
            platform: x._id || "Unknown",
            value: Number((x.v ?? 0).toFixed(2))
        }));
    }

    static async calculateRevenueByMonthStacked(match) {
        const today = new Date();
        const startDate = new Date(today);
        startDate.setMonth(today.getMonth() - 11);

        const startStr = startDate.toISOString().slice(0, 10);
        const endStr = today.toISOString().slice(0, 10);

        const monthsList = [];
        for (let i = 0; i < 12; i++) {
            const d = new Date(startDate.getFullYear(), startDate.getMonth() + i, 1);
            monthsList.push(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`);
        }

        const result = await TblReport2025.aggregate([
            { $match: match },
            {
                $match: {
                    $expr: { $and: [{ $gte: ["$date", startStr] }, { $lte: ["$date", endStr] }] }
                }
            },
            { $addFields: { month: { $substr: ["$date", 0, 7] } } },
            {
                $group: {
                    _id: { month: "$month", retailer: "$retailer" },
                    revenue: { $sum: { $toDouble: "$net_total" } }
                }
            },
            {
                $group: {
                    _id: "$_id.month",
                    platforms: { $push: { name: "$_id.retailer", value: "$revenue" } }
                }
            },
            { $sort: { _id: 1 } }
        ]);

        const revenueMap = new Map(result.map(item => [item._id, item.platforms]));
        return monthsList.map(month => ({
            _id: month,
            platforms: revenueMap.get(month) || []
        }));
    }

    static async calculateTerritoryRevenue(match) {
        const result = await TblReport2025.aggregate([
            { $match: match },
            { $group: { _id: "$territory", revenue: { $sum: { $toDouble: "$net_total" } } } },
            { $project: { _id: 0, territory: "$_id", value: { $round: ["$revenue", 2] } } },
            { $sort: { value: -1 } }
        ]);

        return result;
    }

    static async calculateYearlyStreams(match) {
        const result = await TblReport2025.aggregate([
            { $match: match },
            { $addFields: { year: { $substr: ["$date", 0, 4] } } },
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

        return { data: result, summary: growthInfo };
    }

    static async calculateWeeklyStreams(match) {
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
                    $expr: { $and: [{ $gte: ["$date", startStr] }, { $lte: ["$date", endStr] }] }
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
            { $group: { _id: "$weekday", streams: { $sum: 1 } } }
        ]);

        const weekdayOrder = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
        const streamsMap = new Map(result.map(item => [item._id, item.streams]));

        const finalData = weekdayOrder.map(day => ({
            day,
            streams: streamsMap.get(day) || 0
        }));

        return {
            data: finalData,
            weekRange: `${startOfWeek.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} - ${endOfWeek.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`
        };
    }

    static async calculateMusicStreamComparison(match) {
        const today = new Date();
        const currentYear = today.getFullYear().toString();
        const prevYear = (today.getFullYear() - 1).toString();
        const currentMonthIndex = today.getMonth();

        const result = await TblReport2025.aggregate([
            { $match: match },
            {
                $match: {
                    $expr: { $in: [{ $substr: ["$date", 0, 4] }, [prevYear, currentYear]] }
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
            const { year, month } = item._id;
            if (year === prevYear) prevMap[month] = item.streams;
            if (year === currentYear) currMap[month] = item.streams;
        });

        const labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

        const prevStreams = labels.map((_, i) => prevMap[i + 1] ?? 0);
        const currStreams = labels.map((_, i) => (i > currentMonthIndex ? 0 : currMap[i + 1] ?? 0));

        return {
            labels,
            previousYear: { year: prevYear, streams: prevStreams },
            currentYear: { year: currentYear, streams: currStreams },
            currentMonth: today.toLocaleString('default', { month: 'long' })
        };
    }

    static async calculateStreamingTrendsOverTime(match) {
        const today = new Date();
        const startDate = new Date(today);
        startDate.setMonth(today.getMonth() - 11);

        const startStr = startDate.toISOString().slice(0, 10);
        const endStr = today.toISOString().slice(0, 10);

        const result = await TblReport2025.aggregate([
            { $match: match },
            {
                $match: {
                    $expr: { $and: [{ $gte: ["$date", startStr] }, { $lte: ["$date", endStr] }] }
                }
            },
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
                    distributors: { $push: { name: "$_id.retailer", streams: "$streams" } }
                }
            },
            { $sort: { _id: 1 } }
        ]);

        const allRetailers = new Set();
        result.forEach(item => {
            item.distributors.forEach(d => d.name && allRetailers.add(d.name));
        });

        const retailerList = Array.from(allRetailers).sort();
        const filterOptions = ["All", ...retailerList];

        const monthsList = [];
        let current = new Date(startDate);
        while (current <= today) {
            const y = current.getFullYear();
            const m = String(current.getMonth() + 1).padStart(2, '0');
            monthsList.push(`${y}-${m}`);
            current.setMonth(current.getMonth() + 1);
        }

        const monthMap = new Map(result.map(item => [item._id, item.distributors]));

        const monthlyData = monthsList.map(monthKey => {
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
            const date = new Date(y, m - 1, 1);
            row.displayMonth = date.toLocaleString('default', { month: 'short', year: 'numeric' });

            return row;
        });

        return {
            months: monthlyData.map(m => m.displayMonth),
            monthlyData,
            distributors: filterOptions,
            period: monthlyData.length
                ? `${monthlyData[0].displayMonth} - ${monthlyData[monthlyData.length - 1].displayMonth}`
                : ''
        };
    }

}

module.exports = DashboardService;