package loadshift.core

@LoadshiftDsl
class FanFlowSpec<W : WorkItem, Ctx> @PublishedApi internal constructor(
    codec: WorkItemCodec<W>,
    baseKey: String,
    idgen: IdGen,
    @PublishedApi internal val contextProvider: suspend (Int) -> Ctx,
) : FlowSpec<W>(codec, baseKey, idgen) {
    suspend fun context(): Ctx = contextProvider(0)
}
