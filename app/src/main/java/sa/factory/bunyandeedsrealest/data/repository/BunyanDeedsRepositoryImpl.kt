package sa.factory.bunyandeedsrealest.data.repository

import sa.factory.bunyandeedsrealest.domain.repository.BunyanDeedsRepository
import sa.factory.bunyandeedsrealest.hardware.HardwareSensorDriver
import sa.factory.bunyandeedsrealest.model.BunyanDeedsItem
import sa.factory.bunyandeedsrealest.model.BunyanDeedsOrder
import sa.factory.bunyandeedsrealest.model.BunyanDeedsTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class BunyanDeedsRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : BunyanDeedsRepository {

    private val _catalog = MutableStateFlow<List<BunyanDeedsItem>>(listOf(
        BunyanDeedsItem("1", "مسح تفصيلي 360 درجة عبر Matterport Pro3", "التقاط نقطي عالي الدقة يصل إلى 200 متر مع إعادة بناء الفراغات المعمارية بدقة مليمترية وتوليد مساحات افتراضية تفاعلية", 4500.0, "دقة فائقة", "المسح الليزري المكاني"),
        BunyanDeedsItem("2", "تحليل معدلات التحويل العقاري (CRO Spatial Heatmap)", "تتبع حركة العميل داخل النموذج الافتراضي لتحديد نقاط الجذب البصري وتحسين مسارات واتخاذ قرارات الشراء الفوري", 2800.0, "تحليلي", "النماذج ثلاثية الأبعاد"),
        BunyanDeedsItem("3", "المعاينة الحية عبر الحساسات الليزرية المتزامنة", "بث حي لبيانات المسح المكاني مع قياسات الأعماق والمساحات عبر بروتوكول LiDAR السحابي الآمن", 1950.0, "مباشر", "تحليلات التحويل العقاري")
    ))
    override val catalogFlow: Flow<List<BunyanDeedsItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<BunyanDeedsItem>>(emptyList())
    override val activeCart: StateFlow<List<BunyanDeedsItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<BunyanDeedsTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            BunyanDeedsTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: BunyanDeedsItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: BunyanDeedsItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): BunyanDeedsOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = BunyanDeedsOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
