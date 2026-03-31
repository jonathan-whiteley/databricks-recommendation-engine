import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";

interface ModeToggleProps {
  mode: "known" | "anonymous";
  onModeChange: (mode: "known" | "anonymous") => void;
}

export function ModeToggle({ mode, onModeChange }: ModeToggleProps) {
  return (
    <Tabs value={mode} onValueChange={(v) => onModeChange(v as "known" | "anonymous")}>
      <TabsList>
        <TabsTrigger value="known">Known User</TabsTrigger>
        <TabsTrigger value="anonymous">Anonymous</TabsTrigger>
      </TabsList>
    </Tabs>
  );
}
