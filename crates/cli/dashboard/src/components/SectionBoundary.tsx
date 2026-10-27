import { Component, type ReactNode } from "react";
import { Group } from "./Panel";

interface Props {
  title: string;
  /** Changes on every poll, so a section that could not be drawn retries. */
  resetKey: unknown;
  children: ReactNode;
}

interface State {
  message: string | null;
}

/** What a render threw, as one line. */
export function thrownMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Keeps a section that throws while it renders to its own group: the
 * gateway's body is a passthrough the page cannot trust, and one bad
 * section must not blank the others. React logs what was thrown.
 */
export class SectionBoundary extends Component<Props, State> {
  state: State = { message: null };

  static getDerivedStateFromError(error: unknown): State {
    return { message: thrownMessage(error) };
  }

  componentDidUpdate(prev: Props) {
    if (prev.resetKey !== this.props.resetKey && this.state.message !== null) {
      this.setState({ message: null });
    }
  }

  render() {
    if (this.state.message === null) return this.props.children;
    return (
      <Group title={this.props.title}>
        <p role="alert" className="text-sm text-critical-ink">
          ⛔ This section could not be drawn: {this.state.message}
        </p>
      </Group>
    );
  }
}
