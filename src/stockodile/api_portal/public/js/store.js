/**
 * Stockodile Portal Global State Store
 * public/js/store.js
 */

class GlobalStore {
    constructor() {
        const initialState = {
            walletAddress: "0x7a97970C51812dc3A010C7d01b50e0d17dc79C8",
            activeTxHash: "0x3cd58525b6a71391c5c9f2",
            isConnected: true,
            activePaymentId: null,
            activeFee: "0.10",
            activeRecipient: "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
            activeCurrency: "USD"
        };
        this.listeners = new Set();
        
        // Proxy-based reactive state manager
        this.state = new Proxy(initialState, {
            set: (target, property, value) => {
                if (target[property] !== value) {
                    target[property] = value;
                    this._notify();
                }
                return true;
            }
        });
    }

    getState() {
        return this.state;
    }

    setState(newState) {
        // Triggers Proxy setters reactively for all keys in newState
        Object.assign(this.state, newState);
    }

    subscribe(listener) {
        this.listeners.add(listener);
        // Immediately sync state on subscription
        listener(this.state);
        // Unsubscribe teardown closure
        return () => {
            this.listeners.delete(listener);
        };
    }

    _notify() {
        this.listeners.forEach(listener => {
            try {
                listener(this.state);
            } catch (err) {
                console.error("GlobalStore subscriber error:", err);
            }
        });
    }
}

// Attach to window for global browser availability
window.portalStore = new GlobalStore();
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { GlobalStore };
}
