import { useCallback, useEffect, useState } from 'react'
import MaterialIcon from './MaterialIcon'

export type ToastKind = 'success' | 'error' | 'info'

const STYLES: Record<ToastKind, { border: string; text: string; icon: string }> = {
  success: { border: 'border-primary/30', text: 'text-primary', icon: 'check_circle' },
  error: { border: 'border-tertiary/30', text: 'text-tertiary', icon: 'error' },
  info: { border: 'border-secondary/30', text: 'text-secondary', icon: 'info' },
}

const DISMISS_AFTER_MS = 4000

export interface ToastMessage {
  message: string
  kind: ToastKind
}

/** Holds at most one toast, and clears it after a few seconds. */
export function useToast() {
  const [toast, setToast] = useState<ToastMessage | null>(null)

  useEffect(() => {
    if (!toast) return
    const timer = setTimeout(() => setToast(null), DISMISS_AFTER_MS)
    return () => clearTimeout(timer)
  }, [toast])

  const show = useCallback((message: string, kind: ToastKind = 'success') => {
    setToast({ message, kind })
  }, [])

  const dismiss = useCallback(() => setToast(null), [])
  return { toast, show, dismiss }
}

export default function Toast({ toast, onDismiss }: { toast: ToastMessage; onDismiss: () => void }) {
  const style = STYLES[toast.kind]
  return (
    <div
      role="status"
      className={`animate-fade-in fixed bottom-8 right-8 z-100 flex max-w-md items-center gap-3 rounded-lg border bg-surface-container px-5 py-3 shadow-2xl ${style.border} ${style.text}`}
    >
      <MaterialIcon name={style.icon} />
      <span className="text-xs font-bold uppercase tracking-wider">{toast.message}</span>
      <button onClick={onDismiss} className="ml-2 opacity-60 hover:opacity-100" aria-label="Dismiss">
        <MaterialIcon name="close" className="text-[14px]" />
      </button>
    </div>
  )
}
