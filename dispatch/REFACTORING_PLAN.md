# DISPATCH SERVICE REFACTORING PLAN

## 🎯 **PRIORITY 1: Remove Redundant Code**

### 1. **Consolidate Driver Search Logic**
```typescript
// Create: src/services/shared/DriverSearchService.ts
class DriverSearchService {
  async searchDriversInZone(lat: number, lng: number, zoneIds: string[], radius: number)
  async enrichWithDistances(driverIds: string[], jobLat: number, jobLng: number)
  async filterByAvailability(drivers: Driver[], type: 'free' | 'busy' | 'all')
}
```

### 2. **Extract Common Offer Monitoring**
```typescript
// Create: src/services/shared/OfferMonitorService.ts
class OfferMonitorService {
  async monitorOffers(jobId: string, driverIds: string[], options: MonitorOptions)
  async cancelOffers(jobId: string, driverIds: string[], acceptedDriverId?: string)
}
```

### 3. **Centralize Redis Operations**
```typescript
// Create: src/infrastructure/RedisOperationService.ts
class RedisOperationService {
  async batchSet(operations: RedisOperation[])
  async batchGet(keys: string[])
  async cleanupJob(jobId: string)
}
```

## 🚀 **PRIORITY 2: Flow Optimizations**

### 1. **Implement Caching Layer**
```typescript
// Create: src/services/shared/CacheService.ts
class CacheService {
  async cacheDriverData(jobId: string, drivers: Driver[])
  async getCachedDrivers(jobId: string): Promise<Driver[]>
  async cacheDistances(jobId: string, distances: Map<string, number>)
}
```

### 2. **Optimize Database Queries**
- Batch MongoDB zone queries
- Use Redis pipelines consistently
- Cache zone results per pickup location

### 3. **Reduce Service Dependencies**
```typescript
// Current: Circular dependencies
JobOrchestrator -> DriverMatching -> OfferManagement -> JobOrchestrator

// Target: Linear flow
JobOrchestrator -> DriverSearch -> OfferProcessor -> ResponseHandler
```

## 🔧 **PRIORITY 3: Code Quality**

### 1. **Extract Constants**
```typescript
// Create: src/constants/index.ts
export const TIMEOUTS = {
  OFFER_EXPIRY: 15,
  BATCH_INTERVAL: 45_000,
  KAFKA_TIMEOUT: 5000
}

export const REDIS_KEYS = {
  DRIVER_QUEUE: (jobId: string) => `job:${jobId}:driver_queue`,
  OFFER: (jobId: string, driverId: string) => `offer:${jobId}:${driverId}`
}
```

### 2. **Standardize Error Handling**
```typescript
// Create: src/utils/errorHandler.ts
export function withErrorHandling<T>(
  operation: () => Promise<T>,
  context: string
): Promise<T>
```

### 3. **Remove Duplicate Interfaces**
- Consolidate similar interfaces in types/index.ts
- Remove redundant type definitions

## 📊 **IMPACT ANALYSIS**

### **Before Refactoring:**
- 15+ services with overlapping responsibilities
- 300+ lines of duplicate code
- 5+ different error handling patterns
- Multiple Redis connection patterns

### **After Refactoring:**
- 8 focused services with clear boundaries
- Shared utilities reduce code by ~40%
- Consistent error handling
- Single Redis operation pattern

## 🎯 **IMPLEMENTATION ORDER**

1. **Week 1**: Extract shared services (DriverSearch, OfferMonitor)
2. **Week 2**: Implement caching layer
3. **Week 3**: Consolidate Redis operations
4. **Week 4**: Standardize error handling
5. **Week 5**: Remove redundant code and optimize flows

## 📈 **EXPECTED BENEFITS**

- **Performance**: 30% reduction in database calls
- **Maintainability**: 40% less duplicate code
- **Reliability**: Consistent error handling
- **Testing**: Easier to mock shared services
- **Scalability**: Better separation of concerns