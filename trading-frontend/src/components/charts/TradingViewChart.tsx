import { createChart, CrosshairMode, LineStyle } from 'lightweight-charts'
import { useEffect, useRef, useState } from 'react'

interface ChartProps {
  ticker: string
  period: string
  timeframe: string
  data?: any
  onChartReady?: () => void
}

export const TradingViewChart = ({ticker, period, timeframe, data}: ChartProps) => {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<any>(null)
  const candleSeriesRef = useRef<any>(null)
  const strategySeriesRef = useRef<any>(null)
  const volumeSeriesRef = useRef<any>(null)
  const ma20Ref = useRef<any>(null)
  const ma50Ref = useRef<any>(null)
  const bbUpperRef = useRef<any>(null)
  const bbLowerRef = useRef<any>(null)
  const sseRef = useRef<EventSource | null>(null)
  const initialLoadDoneRef = useRef(false)
  const lastParamsRef = useRef('')

  const [toggles, setToggles] = useState({
    MA20: true,
    MA50: true,
    BB: true,
    Strategy: true,
    Volume: true
  })

  // Helper for BB
  const calculateBollingerBands = (candles: any[], periodLength = 20, multiplier = 2) => {
    const upper = [], lower = []
    for (let i = periodLength - 1; i < candles.length; i++) {
      const slice = candles.slice(i - periodLength + 1, i + 1)
      const closes = slice.map((c: any) => c.close)
      const mean = closes.reduce((a: number, b: number) => a + b) / periodLength
      const variance = closes.reduce((a: number, b: number) => a + Math.pow(b - mean, 2), 0) / periodLength
      const std = Math.sqrt(variance)
      upper.push({ time: candles[i].time, value: mean + multiplier * std })
      lower.push({ time: candles[i].time, value: mean - multiplier * std })
    }
    return { upper, lower }
  }

  // Initialize chart
  useEffect(() => {
    if (!chartContainerRef.current) return

    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: 520,
      layout: {
        background: { color: '#FFFFFF' },
        textColor: '#94A3B8',
        fontFamily: 'JetBrains Mono, monospace',
      },
      localization: {
        timeFormatter: (timestamp: number) => {
          const d = new Date(timestamp * 1000)
          return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
        }
      },
      grid: {
        vertLines: { color: '#F1F5F9', style: LineStyle.Dotted },
        horzLines: { color: '#F1F5F9', style: LineStyle.Dotted },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          width: 1,
          color: '#4F63D2',
          style: LineStyle.Dashed,
          labelBackgroundColor: '#4F63D2',
        },
        horzLine: {
          width: 1,
          color: '#4F63D2',
          labelBackgroundColor: '#4F63D2',
        },
      },
      timeScale: {
        borderColor: '#E2E8F0',
        timeVisible: true,
        secondsVisible: timeframe === '5m' || timeframe === '1min',
      },
      rightPriceScale: {
        borderColor: '#E2E8F0',
        scaleMargins: { top: 0.1, bottom: 0.25 },
      },
      leftPriceScale: {
        visible: true,
        borderColor: '#E2E8F0',
        scaleMargins: { top: 0.1, bottom: 0.25 },
      },
    })

    chartRef.current = chart

    const candleSeries = chart.addCandlestickSeries({
      upColor: '#059669',
      downColor: '#DC2626',
      borderUpColor: '#059669',
      borderDownColor: '#DC2626',
      wickUpColor: '#059669',
      wickDownColor: '#DC2626',
    })
    candleSeriesRef.current = candleSeries

    const strategySeries = chart.addLineSeries({
      color: '#4F63D2',
      lineWidth: 2,
      title: 'Strategy',
      priceScaleId: 'left',
    })
    strategySeriesRef.current = strategySeries

    const volumeSeries = chart.addHistogramSeries({
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
      color: '#4F63D2',
    })
    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.85, bottom: 0 },
    })
    volumeSeriesRef.current = volumeSeries

    ma20Ref.current = chart.addLineSeries({
      color: '#F5A623',
      lineWidth: 1,
      title: 'MA20',
      lineStyle: LineStyle.Solid,
    })
    ma50Ref.current = chart.addLineSeries({
      color: '#06B6D4',
      lineWidth: 1,
      title: 'MA50',
      lineStyle: LineStyle.Solid,
    })

    bbUpperRef.current = chart.addLineSeries({
      color: '#94A3B8',
      lineWidth: 1,
      lineStyle: LineStyle.Dashed,
      title: 'BB Upper',
    })
    bbLowerRef.current = chart.addLineSeries({
      color: '#94A3B8',
      lineWidth: 1,
      lineStyle: LineStyle.Dashed,
      title: 'BB Lower',
    })

    const resizeObserver = new ResizeObserver(entries => {
      if (entries[0]) {
        chart.applyOptions({
          width: entries[0].contentRect.width
        })
      }
    })
    resizeObserver.observe(chartContainerRef.current)

    return () => {
      resizeObserver.disconnect()
      chart.remove()
    }
  }, [timeframe]) // recreate on timeframe change to update timeVisible format

  // Load historical data — only on param changes, not on data refetches
  useEffect(() => {
    const currentParams = `${ticker}|${period}|${timeframe}`
    const isParamChange = currentParams !== lastParamsRef.current
    lastParamsRef.current = currentParams

    // Reset initialLoad flag when params change so chart does a full reload
    if (isParamChange) {
      initialLoadDoneRef.current = false
    }
  }, [ticker, period, timeframe])

  useEffect(() => {
    const loadData = async () => {
      try {
        if (!candleSeriesRef.current || !data) return
        if (!data.market_data || data.market_data.length === 0) return

        // Skip full redraws if we already loaded once for these params
        // SSE handles live candle updates — no need to reset the chart
        if (initialLoadDoneRef.current) return
        initialLoadDoneRef.current = true

        const candleData = data.market_data.map((bar: any) => ({
          time: bar.time,
          open: bar.open,
          high: bar.high,
          low: bar.low,
          close: bar.close,
        }))
        // Lightweight charts requires unique strictly ascending times
        // Deduplicate
        const uniqueCandles: any[] = []
        const seenTimes = new Set()
        candleData.forEach((c: any) => {
           if(!seenTimes.has(c.time)) {
               uniqueCandles.push(c)
               seenTimes.add(c.time)
           }
        })
        uniqueCandles.sort((a,b) => a.time - b.time)

        candleSeriesRef.current.setData(uniqueCandles)

        const volumeData = uniqueCandles.map((bar: any) => ({
          time: bar.time,
          value: bar.volume || 1,
          color: bar.close >= bar.open ? '#05966920' : '#DC262620',
        }))
        volumeSeriesRef.current.setData(volumeData)

        if (data.strategy_data?.length > 0) {
          const startPrice = uniqueCandles[0]?.close || 1
          const strategyLine = data.strategy_data.map((d: any) => ({
            time: d.time,
            value: startPrice * (1 + d.strategy_value / 100),
          }))
          const uniqueStrategy: any[] = []
          const seenStrategyTimes = new Set()
          strategyLine.forEach((c: any) => {
             if(!seenStrategyTimes.has(c.time)) {
                 uniqueStrategy.push(c)
                 seenStrategyTimes.add(c.time)
             }
          })
          uniqueStrategy.sort((a,b) => a.time - b.time)
          strategySeriesRef.current.setData(uniqueStrategy)
        } else {
            strategySeriesRef.current.setData([])
        }

        if (uniqueCandles.length > 50) {
          const closes = uniqueCandles.map((c: any) => c.close)
          const ma20 = uniqueCandles.slice(19).map((c: any, i: number) => ({
            time: c.time,
            value: closes.slice(i, i + 20).reduce((a: number, b: number) => a + b) / 20
          }))
          const ma50 = uniqueCandles.slice(49).map((c: any, i: number) => ({
            time: c.time,
            value: closes.slice(i, i + 50).reduce((a: number, b: number) => a + b) / 50
          }))
          ma20Ref.current.setData(ma20)
          ma50Ref.current.setData(ma50)

          const bbData = calculateBollingerBands(uniqueCandles, 20, 2)
          bbUpperRef.current.setData(bbData.upper)
          bbLowerRef.current.setData(bbData.lower)
        }

        if (data.trade_markers?.length > 0) {
          const markers = data.trade_markers.map((m: any) => ({
            time: m.time,
            position: m.action === 'entry' ? 'belowBar' : 'aboveBar',
            color: m.action === 'entry' ? '#F5A623' : '#DC2626',
            shape: m.action === 'entry' ? 'arrowUp' : 'arrowDown',
            text: m.action === 'entry'
              ? `Buy $${m.price.toFixed(2)}`
              : `Sell $${m.price.toFixed(2)}`,
          }))
          const validMarkers = markers.filter((m: any) => seenTimes.has(m.time))
          validMarkers.sort((a: any, b: any) => a.time - b.time)
          candleSeriesRef.current.setMarkers(validMarkers)
        }

        chartRef.current?.timeScale().fitContent()
      } catch (e) {
          console.error("Failed to load chart data:", e)
      }
    }

    loadData()
  }, [ticker, period, timeframe, data])

  // REAL-TIME SSE connection
  useEffect(() => {
    if (sseRef.current) {
      sseRef.current.close()
    }

    const sseUrl = `http://localhost:8001/realtime/stream/prices/${ticker}?timeframe=${timeframe}`
    const sse = new EventSource(sseUrl)
    sseRef.current = sse

    sse.onmessage = (event) => {
      const bar = JSON.parse(event.data)
      if (bar.error) return

      try {
        candleSeriesRef.current?.update({
          time: bar.time,
          open: bar.open,
          high: bar.high,
          low: bar.low,
          close: bar.close,
        })

        volumeSeriesRef.current?.update({
          time: bar.time,
          value: bar.volume || 1,
          color: bar.close >= bar.open ? '#05966920' : '#DC262620',
        })
      } catch(e) {
          // Lightweight charts throws if we update a time in the past
      }
    }

    return () => {
      sse.close()
    }
  }, [ticker, timeframe])

  // Apply visibility toggles
  useEffect(() => {
      if(ma20Ref.current) ma20Ref.current.applyOptions({visible: toggles.MA20})
      if(ma50Ref.current) ma50Ref.current.applyOptions({visible: toggles.MA50})
      if(bbUpperRef.current) bbUpperRef.current.applyOptions({visible: toggles.BB})
      if(bbLowerRef.current) bbLowerRef.current.applyOptions({visible: toggles.BB})
      if(strategySeriesRef.current) strategySeriesRef.current.applyOptions({visible: toggles.Strategy})
      if(volumeSeriesRef.current) volumeSeriesRef.current.applyOptions({visible: toggles.Volume})
  }, [toggles])

  return (
    <div>
      <div ref={chartContainerRef} className="w-full relative" />
      <div className="flex gap-2 mt-4 text-xs font-medium">
        {Object.entries(toggles).map(([k, v]) => (
            <button key={k} onClick={() => setToggles({...toggles, [k]: !v})}
                className={`px-2 py-1 rounded-md border transition-colors ${v ? 'bg-indigo-50 border-indigo-200 text-indigo-700' : 'bg-white border-slate-200 text-slate-400'}`}>
                {k}
            </button>
        ))}
      </div>
    </div>
  )
}
