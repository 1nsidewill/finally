// src/components/Button/Button.test.tsx
import {
  RedButton,
  BlueButton,
  SmButton,
} from "../Button.stories"; 
import { render, screen } from "@testing-library/react";

const mainColor = "brown";

test("should render RedButton", () => {
  render(<RedButton {...RedButton.args} />);
  expect(screen.getByRole("button")).toHaveTextContent(/Red/i);
  expect(screen.getByRole("button")).toHaveStyle("backgroundColor: red");
});

test("should render BlueButton", () => {
  render(<BlueButton {...BlueButton.args} />);
  expect(screen.getByRole("button")).toHaveTextContent(/Blue/i);
  expect(screen.getByRole("button")).toHaveStyle("backgroundColor: blue");
});

test("should render SmButton", () => {
  render(<SmButton {...SmButton.args} />);
  expect(screen.getByRole("button")).toHaveTextContent(/Small Button/i);
  expect(screen.getByRole("button")).toHaveStyle("backgroundColor: gray");
});
