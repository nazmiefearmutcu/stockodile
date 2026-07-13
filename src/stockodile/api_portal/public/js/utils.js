/**
 * Stockodile Portal Shared Utility Functions
 * public/js/utils.js
 */

/**
 * Escape untrusted strings for safe insertion into HTML (innerHTML sinks).
 * Converts &, <, >, ", and ' to HTML entities.
 */
function escapeHtml(unsafe) {
    if (unsafe == null) {
        return '';
    }
    return String(unsafe)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

/**
 * Returns time string formatted as "hh:mm:ss A" using GMT+3.
 */
function getSyncedTime(dateInput = null) {
    if (typeof dateInput === 'string' && /^\d{2}:\d{2}:\d{2}\s+(AM|PM)$/i.test(dateInput)) {
        return dateInput;
    }
    
    const date = dateInput ? new Date(dateInput) : new Date();
    
    if (isNaN(date.getTime())) {
        return "12:00:00 AM";
    }

    // Convert current client time to GMT+3 (Istanbul Timezone equivalent)
    const utc = date.getTime() + (date.getTimezoneOffset() * 60000);
    const targetOffset = 3 * 3600000; // +3 hours
    const targetDate = new Date(utc + targetOffset);

    let hours = targetDate.getHours();
    const minutes = String(targetDate.getMinutes()).padStart(2, '0');
    const seconds = String(targetDate.getSeconds()).padStart(2, '0');
    const ampm = hours >= 12 ? 'PM' : 'AM';
    hours = hours % 12;
    hours = hours ? hours : 12; // '0' becomes '12'
    const hoursStr = String(hours).padStart(2, '0');
    
    return `${hoursStr}:${minutes}:${seconds} ${ampm}`;
}

/**
 * Generates a valid 66-character mock signature starting with 0x and containing exactly 64 hex characters.
 */
function generateValidMockSignature() {
    let result = "0x";
    for (let i = 0; i < 64; i++) {
        result += Math.floor(Math.random() * 16).toString(16);
    }
    return result;
}

/**
 * Generates an array of time-series data entries with random walk deviation.
 */
function generateTimeSeriesData(basePrice, length = 20) {
    const data = [];
    let currentPrice = basePrice;
    const now = Date.now();
    const intervalMs = 2000;

    for (let i = 0; i < length; i++) {
        const timeVal = now - (length - 1 - i) * intervalMs;
        
        if (i > 0) {
            // Random walk: max ±0.1% deviation from the previous step's price
            const deviationPercent = (Math.random() - 0.5) * 2 * 0.001; // Range: [-0.001, 0.001]
            currentPrice = currentPrice * (1 + deviationPercent);
        }

        // Never drop below 50% of the base price
        if (currentPrice < basePrice * 0.5) {
            currentPrice = basePrice * 0.5;
        }

        data.push({
            time: getSyncedTime(timeVal),
            price: parseFloat(currentPrice.toFixed(2))
        });
    }
    return data;
}

// Expose utilities on window object for browser compatibility
window.escapeHtml = escapeHtml;
window.getSyncedTime = getSyncedTime;
window.generateValidMockSignature = generateValidMockSignature;
window.generateMockSignature = generateValidMockSignature; // Compatibility mapping for generateMockSignature
window.generateTimeSeriesData = generateTimeSeriesData;

if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        escapeHtml,
        getSyncedTime,
        generateValidMockSignature,
        generateTimeSeriesData
    };
}
