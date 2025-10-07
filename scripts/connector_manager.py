#!/usr/bin/env python3
"""
Connector Management CLI Tool.

This tool provides command-line management capabilities for connectors
in the 254Carbon ingestion platform.
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

import click
import yaml
import httpx
from rich.console import Console
from rich.table import Table
from rich import print as rprint


console = Console()


class ConnectorManager:
    """Manager for connector operations."""

    def __init__(self, registry_url: str = "http://localhost:8500"):
        """
        Initialize connector manager.

        Args:
            registry_url: URL of the connector registry service
        """
        self.registry_url = registry_url
        self.connectors_dir = Path("connectors")

    async def list_connectors(self) -> List[Dict[str, Any]]:
        """List all available connectors."""
        connectors = []

        # Scan connectors directory
        for connector_dir in self.connectors_dir.iterdir():
            if connector_dir.is_dir() and connector_dir.name not in ["base", "__pycache__"]:
                connector_yaml = connector_dir / "connector.yaml"

                if connector_yaml.exists():
                    with open(connector_yaml, 'r') as f:
                        config = yaml.safe_load(f)
                        connectors.append({
                            "name": config.get("name"),
                            "version": config.get("version"),
                            "market": config.get("market"),
                            "mode": config.get("mode"),
                            "enabled": config.get("enabled", False),
                            "schedule": config.get("schedule"),
                            "path": str(connector_dir)
                        })

        return connectors

    async def get_connector(self, name: str) -> Optional[Dict[str, Any]]:
        """Get connector configuration."""
        connector_yaml = self.connectors_dir / name / "connector.yaml"

        if not connector_yaml.exists():
            return None

        with open(connector_yaml, 'r') as f:
            return yaml.safe_load(f)

    async def enable_connector(self, name: str) -> bool:
        """Enable a connector."""
        connector_yaml = self.connectors_dir / name / "connector.yaml"

        if not connector_yaml.exists():
            return False

        with open(connector_yaml, 'r') as f:
            config = yaml.safe_load(f)

        config["enabled"] = True

        with open(connector_yaml, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)

        return True

    async def disable_connector(self, name: str) -> bool:
        """Disable a connector."""
        connector_yaml = self.connectors_dir / name / "connector.yaml"

        if not connector_yaml.exists():
            return False

        with open(connector_yaml, 'r') as f:
            config = yaml.safe_load(f)

        config["enabled"] = False

        with open(connector_yaml, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)

        return True

    async def test_connector(self, name: str) -> Dict[str, Any]:
        """Test a connector."""
        try:
            # Try to query the connector registry service
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.registry_url}/connectors/{name}")

                if response.status_code == 200:
                    return {
                        "status": "success",
                        "connector": response.json()
                    }
                else:
                    return {
                        "status": "error",
                        "message": f"Connector not found in registry (HTTP {response.status_code})"
                    }

        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to test connector: {str(e)}"
            }

    async def validate_connector(self, name: str) -> Dict[str, Any]:
        """Validate connector configuration."""
        connector_yaml = self.connectors_dir / name / "connector.yaml"

        if not connector_yaml.exists():
            return {
                "valid": False,
                "errors": [f"Connector {name} not found"]
            }

        errors = []

        try:
            with open(connector_yaml, 'r') as f:
                config = yaml.safe_load(f)

            # Validate required fields
            required_fields = ["name", "version", "market", "mode", "output_topic", "schema"]

            for field in required_fields:
                if field not in config:
                    errors.append(f"Missing required field: {field}")

            # Validate schema file exists
            schema_path = self.connectors_dir / name / config.get("schema", "")
            if not schema_path.exists():
                errors.append(f"Schema file not found: {config.get('schema')}")

            # Validate mode
            valid_modes = ["batch", "streaming", "hybrid"]
            if config.get("mode") not in valid_modes:
                errors.append(f"Invalid mode: {config.get('mode')} (must be one of {valid_modes})")

            return {
                "valid": len(errors) == 0,
                "errors": errors,
                "config": config
            }

        except Exception as e:
            return {
                "valid": False,
                "errors": [f"Failed to validate connector: {str(e)}"]
            }


# CLI Commands

@click.group()
@click.option('--registry-url', default="http://localhost:8500", help="Connector registry service URL")
@click.pass_context
def cli(ctx, registry_url):
    """Connector Management CLI Tool."""
    ctx.ensure_object(dict)
    ctx.obj['manager'] = ConnectorManager(registry_url)


@cli.command()
@click.pass_context
def list(ctx):
    """List all connectors."""
    manager = ctx.obj['manager']
    connectors = asyncio.run(manager.list_connectors())

    if not connectors:
        console.print("[yellow]No connectors found[/yellow]")
        return

    table = Table(title="Available Connectors")
    table.add_column("Name", style="cyan")
    table.add_column("Version", style="magenta")
    table.add_column("Market", style="green")
    table.add_column("Mode", style="blue")
    table.add_column("Enabled", style="yellow")
    table.add_column("Schedule")

    for connector in connectors:
        table.add_row(
            connector["name"],
            connector["version"],
            connector["market"],
            connector["mode"],
            "✓" if connector["enabled"] else "✗",
            connector.get("schedule", "N/A")
        )

    console.print(table)


@cli.command()
@click.argument('name')
@click.pass_context
def info(ctx, name):
    """Show detailed information about a connector."""
    manager = ctx.obj['manager']
    connector = asyncio.run(manager.get_connector(name))

    if not connector:
        console.print(f"[red]Connector '{name}' not found[/red]")
        sys.exit(1)

    rprint(f"[bold cyan]Connector: {name}[/bold cyan]")
    rprint(yaml.dump(connector, default_flow_style=False))


@cli.command()
@click.argument('name')
@click.pass_context
def enable(ctx, name):
    """Enable a connector."""
    manager = ctx.obj['manager']
    success = asyncio.run(manager.enable_connector(name))

    if success:
        console.print(f"[green]✓ Connector '{name}' enabled[/green]")
    else:
        console.print(f"[red]✗ Failed to enable connector '{name}'[/red]")
        sys.exit(1)


@cli.command()
@click.argument('name')
@click.pass_context
def disable(ctx, name):
    """Disable a connector."""
    manager = ctx.obj['manager']
    success = asyncio.run(manager.disable_connector(name))

    if success:
        console.print(f"[yellow]✓ Connector '{name}' disabled[/yellow]")
    else:
        console.print(f"[red]✗ Failed to disable connector '{name}'[/red]")
        sys.exit(1)


@cli.command()
@click.argument('name')
@click.pass_context
def test(ctx, name):
    """Test a connector."""
    manager = ctx.obj['manager']

    console.print(f"Testing connector '{name}'...")
    result = asyncio.run(manager.test_connector(name))

    if result["status"] == "success":
        console.print(f"[green]✓ Connector '{name}' test passed[/green]")
        rprint(result.get("connector", {}))
    else:
        console.print(f"[red]✗ Connector '{name}' test failed[/red]")
        console.print(f"Error: {result.get('message')}")
        sys.exit(1)


@cli.command()
@click.argument('name')
@click.pass_context
def validate(ctx, name):
    """Validate connector configuration."""
    manager = ctx.obj['manager']

    console.print(f"Validating connector '{name}'...")
    result = asyncio.run(manager.validate_connector(name))

    if result["valid"]:
        console.print(f"[green]✓ Connector '{name}' configuration is valid[/green]")
    else:
        console.print(f"[red]✗ Connector '{name}' configuration is invalid[/red]")
        for error in result["errors"]:
            console.print(f"  - {error}")
        sys.exit(1)


@cli.command()
@click.pass_context
def validate_all(ctx):
    """Validate all connectors."""
    manager = ctx.obj['manager']
    connectors = asyncio.run(manager.list_connectors())

    results = []
    for connector in connectors:
        result = asyncio.run(manager.validate_connector(connector["name"]))
        results.append({
            "name": connector["name"],
            "valid": result["valid"],
            "errors": result.get("errors", [])
        })

    # Display results
    table = Table(title="Connector Validation Results")
    table.add_column("Name", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Errors")

    for result in results:
        status = "✓ Valid" if result["valid"] else "✗ Invalid"
        errors = "\n".join(result["errors"]) if result["errors"] else "-"

        table.add_row(result["name"], status, errors)

    console.print(table)

    # Exit with error if any validation failed
    if any(not r["valid"] for r in results):
        sys.exit(1)


if __name__ == "__main__":
    cli(obj={})
