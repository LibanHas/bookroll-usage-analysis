document.addEventListener('DOMContentLoaded', function() {
    console.log('Student Analytics Dashboard initializing...');

    // Time frame selector event handler
    const timeFrameSelect = document.getElementById('time-frame-select');
    if (timeFrameSelect) {
        timeFrameSelect.addEventListener('change', function() {
            const selectedTimeFrame = this.value;
            console.log('Time frame changed to:', selectedTimeFrame);

            // Show loading indicator
            const loadingIndicator = document.createElement('div');
            loadingIndicator.className = 'fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50';
            loadingIndicator.innerHTML = `
                <div class="bg-white dark:bg-gray-800 rounded-lg p-6 flex items-center gap-3">
                    <div class="animate-spin rounded-full h-6 w-6 border-b-2 border-primary-500"></div>
                    <span class="text-gray-700 dark:text-gray-300">Loading analytics...</span>
                </div>
            `;
            document.body.appendChild(loadingIndicator);

            // Reload the page with the new time frame parameter
            const url = new URL(window.location);
            url.searchParams.set('time_frame', selectedTimeFrame);
            window.location.href = url.toString();
        });
    }

    // Initialize default data structures
    let analyticsData = {
        overall_stats: {
            total_students: 0,
            total_activities: 0,
            avg_activities: 0,
            std_dev_activities: 0
        },
        activity_distribution: [],
        top_operations: [],
        daily_trends: []
    };

    let learningInsightsData = {
        engagement_levels: [],
        content_interactions: []
    };

    // Parse data from hidden script tags with comprehensive error handling
    try {
        const analyticsElement = document.getElementById('analytics-data');
        if (analyticsElement && analyticsElement.textContent.trim()) {
            const parsedData = JSON.parse(analyticsElement.textContent);
            if (parsedData && typeof parsedData === 'object') {
                analyticsData = { ...analyticsData, ...parsedData };
                console.log('Analytics data loaded:', analyticsData);
            }
        }
    } catch (e) {
        console.warn('Failed to parse analytics data:', e);
    }

    try {
        const insightsElement = document.getElementById('learning-insights-data');
        if (insightsElement && insightsElement.textContent.trim()) {
            const parsedData = JSON.parse(insightsElement.textContent);
            if (parsedData && typeof parsedData === 'object') {
                learningInsightsData = { ...learningInsightsData, ...parsedData };
                console.log('Learning insights data loaded:', learningInsightsData);
            }
        }
    } catch (e) {
        console.warn('Failed to parse learning insights data:', e);
    }

    // Helper function to safely render charts
    function renderChart(elementId, options, fallbackMessage) {
        try {
            const element = document.querySelector(elementId);
            if (!element) {
                console.error(`Element ${elementId} not found`);
                return;
            }

            // Clear any existing content
            element.innerHTML = '';

            const chart = new ApexCharts(element, options);
            chart.render().then(() => {
                console.log(`Chart ${elementId} rendered successfully`);
            }).catch((error) => {
                console.error(`Error rendering chart ${elementId}:`, error);
                element.innerHTML = `<div class="flex items-center justify-center h-80 text-red-500">${fallbackMessage}</div>`;
            });
        } catch (error) {
            console.error(`Error initializing chart ${elementId}:`, error);
            const element = document.querySelector(elementId);
            if (element) {
                element.innerHTML = `<div class="flex items-center justify-center h-80 text-red-500">${fallbackMessage}</div>`;
            }
        }
    }

    // Activity Distribution Chart
    if (analyticsData.activity_distribution && analyticsData.activity_distribution.length > 0) {
        const activityDistributionOptions = {
            series: [{
                name: 'Students',
                data: analyticsData.activity_distribution.map(item => ({
                    x: item.range || 'Unknown',
                    y: item.count || 0
                }))
            }],
            chart: {
                type: 'bar',
                height: 320,
                toolbar: { show: false },
                background: 'transparent'
            },
            plotOptions: {
                bar: {
                    borderRadius: 4,
                    horizontal: false,
                    columnWidth: '60%'
                }
            },
            dataLabels: {
                enabled: true,
                style: {
                    fontSize: '12px'
                }
            },
            xaxis: {
                title: {
                    text: 'Activity Range',
                    style: { fontSize: '14px' }
                },
                labels: {
                    style: { fontSize: '12px' }
                }
            },
            yaxis: {
                title: {
                    text: 'Number of Students',
                    style: { fontSize: '14px' }
                },
                labels: {
                    style: { fontSize: '12px' }
                }
            },
            colors: ['#3B82F6'],
            title: {
                text: 'Student Distribution by Activity Level',
                align: 'left',
                style: { fontSize: '16px', fontWeight: 'bold' }
            },
            grid: {
                borderColor: '#e7e7e7',
                strokeDashArray: 3
            }
        };
        renderChart("#activity-distribution-chart", activityDistributionOptions, "Error loading activity distribution chart");
    } else {
        document.querySelector("#activity-distribution-chart").innerHTML =
            '<div class="flex items-center justify-center h-80 text-gray-500"><div class="text-center"><i class="ri-bar-chart-line text-4xl mb-2"></i><p>No activity distribution data available</p></div></div>';
    }

    // Engagement Levels Chart
    if (learningInsightsData.engagement_levels && learningInsightsData.engagement_levels.length > 0) {
        const engagementLevelsOptions = {
            series: learningInsightsData.engagement_levels.map(item => item.student_count || 0),
            chart: {
                type: 'donut',
                height: 320,
                background: 'transparent'
            },
            labels: learningInsightsData.engagement_levels.map(item => item.level || 'Unknown'),
            colors: ['#10B981', '#3B82F6', '#F59E0B', '#EF4444'],
            legend: {
                position: 'bottom',
                fontSize: '12px'
            },
            title: {
                text: 'Student Engagement Distribution',
                align: 'left',
                style: { fontSize: '16px', fontWeight: 'bold' }
            },
            dataLabels: {
                enabled: true,
                style: { fontSize: '12px' }
            },
            plotOptions: {
                pie: {
                    donut: {
                        size: '60%'
                    }
                }
            }
        };
        renderChart("#engagement-levels-chart", engagementLevelsOptions, "Error loading engagement levels chart");
    } else {
        document.querySelector("#engagement-levels-chart").innerHTML =
            '<div class="flex items-center justify-center h-80 text-gray-500"><div class="text-center"><i class="ri-pie-chart-line text-4xl mb-2"></i><p>No engagement data available</p></div></div>';
    }

    // Daily Trends Chart
    if (analyticsData.daily_trends && analyticsData.daily_trends.length > 0) {
        const dailyTrendsOptions = {
            series: [{
                name: 'Active Students',
                type: 'column',
                data: analyticsData.daily_trends.map(item => ({
                    x: item.date || 'Unknown',
                    y: item.active_students || 0
                }))
            }, {
                name: 'Total Activities',
                type: 'line',
                data: analyticsData.daily_trends.map(item => ({
                    x: item.date || 'Unknown',
                    y: item.total_activities || 0
                }))
            }],
            chart: {
                height: 384,
                type: 'line',
                toolbar: { show: false },
                background: 'transparent'
            },
            stroke: {
                width: [0, 4],
                curve: 'smooth'
            },
            dataLabels: { enabled: false },
            xaxis: {
                type: 'datetime',
                title: {
                    text: 'Date',
                    style: { fontSize: '14px' }
                },
                labels: {
                    style: { fontSize: '12px' }
                }
            },
            yaxis: [{
                title: {
                    text: 'Active Students',
                    style: { fontSize: '14px' }
                },
                labels: {
                    style: { fontSize: '12px' }
                }
            }, {
                opposite: true,
                title: {
                    text: 'Total Activities',
                    style: { fontSize: '14px' }
                },
                labels: {
                    style: { fontSize: '12px' }
                }
            }],
            colors: ['#3B82F6', '#10B981'],
            title: {
                text: 'Daily Learning Activity Trends',
                align: 'left',
                style: { fontSize: '16px', fontWeight: 'bold' }
            },
            grid: {
                borderColor: '#e7e7e7',
                strokeDashArray: 3
            },
            legend: {
                position: 'top',
                fontSize: '12px'
            }
        };
        renderChart("#daily-trends-chart", dailyTrendsOptions, "Error loading daily trends chart");
    } else {
        document.querySelector("#daily-trends-chart").innerHTML =
            '<div class="flex items-center justify-center h-96 text-gray-500"><div class="text-center"><i class="ri-line-chart-line text-4xl mb-2"></i><p>No daily trends data available</p></div></div>';
    }

    // Initialize counter animations with proper error handling
    document.querySelectorAll('.counter-value').forEach(counter => {
        const dataValue = counter.getAttribute('data-value');
        let target = 0;

        // Handle various data types and edge cases
        if (dataValue && dataValue !== 'None' && dataValue !== 'null' && dataValue !== 'undefined') {
            const parsed = parseFloat(dataValue);
            if (!isNaN(parsed)) {
                target = parsed;
            }
        }

        // Set initial value
        counter.textContent = target;

        // Animate if target is reasonable
        if (target > 0 && target < 10000) {
            let current = 0;
            const increment = target / 50;
            const timer = setInterval(() => {
                current += increment;
                if (current >= target) {
                    counter.textContent = target % 1 === 0 ? target : target.toFixed(2);
                    clearInterval(timer);
                } else {
                    counter.textContent = Math.floor(current);
                }
            }, 20);
        }
    });

    console.log('Student Analytics Dashboard initialization complete');
});